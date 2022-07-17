from datetime import datetime, timedelta, timezone
import os
import urllib.parse

import prefect
from prefect import task, Flow
import requests
import mysql.connector


TOTAL_TWEETS_TO_RETRIEVE = 10


@task(log_stdout=True)
def retrieve_tweets():
    logger = prefect.context.get("logger")
    auth_token = os.getenv('TWITTER_BEARER_TOKEN')
    logger.info('about to request tweets')
    # time format: YYYY-MM-DDTHH:mm:ssZ (ISO 8601/RFC 3339)
    fetch_next = True
    next_token = None
    results = []
    while fetch_next:
        response = requests.get(
            'https://api.twitter.com/2/tweets/search/recent',
            params={
                'end_time': (datetime.now(timezone.utc) - timedelta(days=5)).isoformat(),
                'query': '"data science" has:links',
                'tweet.fields': 'entities',
                'max_results': '10',
                'next_token': next_token,
            },
            headers={
                'Authorization': f'Bearer {auth_token}',
            },
        )
        response_data = response.json()
        results.extend(response_data['data'])
        # logger.info(f'data:{json.dumps(results)}')
        next_token = response_data['meta'].get('next_token')
        logger.info(f'next token: {next_token}')
        logger.info(f'results length: {len(results)}')
        fetch_next = True if next_token and len(results) < TOTAL_TWEETS_TO_RETRIEVE else False
    logger.info(f'got {len(results)} tweets')
    return results


@task(log_stdout=True)
def extract_hostname_from_tweet(tweet):
    if 'urls' in tweet['entities']:
        for url in tweet['entities']['urls']:
            if 'unwound_url' in url:
                tweet['hostname'] = urllib.parse.urlparse(url['unwound_url']).hostname
            else:
                tweet['hostname'] = urllib.parse.urlparse(url['expanded_url']).hostname
    return tweet


@task(log_stdout=True)
def filter_tweets_without_hostnames(original_tweets):
    return list(filter(lambda x: 'hostname' in x, original_tweets))


@task(log_stdout=True)
def get_conversation(response):
    logger = prefect.context.get("logger")
    auth_token = os.getenv('TWITTER_BEARER_TOKEN')
    logger.info('about to request conversation')
    # this is a list comprehension
    # they can get really complicated, but for readability, you should only
    # use it for simply constructing lists
    response_tweet_ids = [tweet['id'] for tweet in response['data']]
    # join explanation:
    #   my_array = [1, 2, 3]
    #   my_array_string = '--'.join(my_array)
    #   print(my_array_string)
    #   >>> 1--2--3
    conversation_tweets = dict()
    # Get tweets in the conversation for each of our original tweets
    for tweet_id in response_tweet_ids:
        conversation_tweets_response = requests.get(
            'https://api.twitter.com/2/tweets/search/recent',
            params={
                'query': f'conversation_id:{tweet_id}',
                'tweet.fields': 'conversation_id',
            },
            headers={
                'Authorization': f'Bearer {auth_token}',
            },
        )
        # logger.info(f'conversation:{conversation_tweets_response}')
        conversation_tweet_data = conversation_tweets_response.json()
        # logger.info(f'response: {conversation_tweet_data}')
        # Skip the original tweet record if there are no tweets in the conversation
        if conversation_tweet_data['meta']['result_count'] == 0:
            continue
        # loop through our conversation search results
        for tweet in conversation_tweet_data['data']:
            # and create a dictionary of the results where the key is the conversation id
            # and the value is a list of the text from the tweets in the conversation
            if tweet['conversation_id'] not in conversation_tweets:
                conversation_tweets[tweet['conversation_id']] = []
            conversation_tweets[tweet['conversation_id']].append(tweet['text'])
        logger.info(f'conversation length for tweet id {tweet_id} -- {len(conversation_tweet_data["data"])}')
    logger.debug(f'data:{conversation_tweets}')
    logger.info(f'got {len(conversation_tweets)} and put as a new dictionary ')
    logger.info(f'{conversation_tweets}')
    # the conversation ID the same as the ID of the tweet that started the conversation
    # so we can take all the tweets text for a conversation ID and store them alongside
    # the original tweet.
    # later, we can do sentiment analysis against all the conversation text for a given
    # tweet all at once.
    for record in response['data']:
        # if the tweet's ID is not in the conversation dictionary, just store an empty list
        record['conversation_text'] = conversation_tweets.get(record['id'])
    return response


@task(log_stdout=True)
def get_conversation_for_tweet(tweet):
    logger = prefect.context.get("logger")
    auth_token = os.getenv('TWITTER_BEARER_TOKEN')
    tweet_id = tweet['id']
    logger.info('about to request conversation')
    conversation_tweets_response = requests.get(
        'https://api.twitter.com/2/tweets/search/recent',
        params={
            'query': f'conversation_id:{tweet_id}',
            'tweet.fields': 'conversation_id',
        },
        headers={
            'Authorization': f'Bearer {auth_token}',
        },
    )
    # logger.info(f'conversation:{conversation_tweets_response}')
    conversation_tweet_data = conversation_tweets_response.json()
    logger.info(f'response: {conversation_tweet_data}')
    # Skip the original tweet record if there are no tweets in the conversation
    if conversation_tweet_data['meta']['result_count'] == 0:
        tweet['conversation_texts'] = []
    else:
        # in the original tweet, store the text content of all the conversation tweets
        # in some "conversation_texts" key of the tweet dictionary
        tweet['conversation_texts'] = [result_tweet['text']
                                       for result_tweet in conversation_tweet_data['data']]
    return tweet


@task(log_stdout=True)
def transform_conversation_tweet(response):
    logger = prefect.context.get("logger")
    logger.info('transforming tweet data')
    data = [[tweet['data']['id'], tweet['data']['text'], tweet['counts_tweets_for_hosts']] for tweet in response]
    logger.info(f'reformatted {len(data)} records')
    logger.info(f'{data}')
    return data


@task(log_stdout=True)
def perform_sentiment_analysis(conversation):
    # TODO: actually do sentiment analysis
    conversation['sentiment_score'] = 0
    return conversation


@task(log_stdout=True)
def count_hostname(original_tweets):
    logger = prefect.context.get("logger")
    logger.info('classifying tweet data with links')
    count_tweets_for_hosts = dict()
    for tweet in original_tweets:
        # dictionaries have a method called 'get'
        # get() accepts a first parameter as the key to retrieve a value for
        # it accepts an optional second parameter, which is the value to return
        #   if the requested key is not in the dictionary
        # if no second parameter is provided, and the key does not exist in the dictionary,
        #   then it returns None
        # if you try to access a dictionary value using a key that isn't in the dictionary
        #   using the dictionary['key'] syntax, you'll hit a KeyError
        hostname = tweet.get('hostname')
        if hostname not in count_tweets_for_hosts:
            count_tweets_for_hosts[hostname] = {
                'count': 1,
                'original_tweets': [tweet['id']]
            }
            # {
            #   'google.com': {
            #       'count': 1,
            #       'original_tweets': [12423451345, 3567456748]
            #    },
            #   'netflix.com': {
            #       'count': 1,
            #       'original_tweets': [12423451345, 3567456748]
            #    }
            #  }
        elif hostname is not None:
            count_tweets_for_hosts[hostname]['count'] += 1
            count_tweets_for_hosts[hostname]['original_tweets'].append(tweet['id'])
    logger.info(f'added {len(count_tweets_for_hosts)} as a dictionary')
    logger.info(f'added {count_tweets_for_hosts} as a dictionary')
    return count_tweets_for_hosts


@task(log_stdout=True)
def store_hostname(hostname_data):
    logger = prefect.context.get("logger")
    logger.info('connecting to database')
    connection = mysql.connector.connect(
        host=os.getenv('DATABASE_HOST', 'database'),
        user=os.getenv('DATABASE_USER', 'root'),
        password=os.getenv('DATABASE_PASSWORD', 'mariadb'),
        database=os.getenv('DATABASE_SCHEMA', 'twitter'),
    )
    cursor = connection.cursor()
    sql = 'insert into hostname(hostname, counting) values (%s, %s)'
    logger.info('inserting hostname data')
    # Format looks like this:
    # {
    #     'counts_tweets_for_hosts': {
    #         'www.researchsquare.com': {
    #             'count': 1
    #         },
    #         'statds.org': {
    #             'count': 1
    #         }
    #     }
    # }
    for hostname, hostname_count_value in hostname_data.items():
        params = (hostname, hostname_count_value['count'])
        cursor.execute(sql, params)
        # can't use executemany because we need to get the inserted ID for reach record
        # and we can only get the ID for the last inserted record
        hostname_data[hostname]['id'] = cursor.lastrowid
        # {
        #   'google.com': {
        #       'count': 1,
        #       'id': 10
        #    },
        #   'netflix.com': {
        #       'count': 1,
        #       'id': 11
        #    }
        #  }
        # later, we can get the id in a way like
        # hostname_data[hostname]['id']
    connection.commit()


@task(log_stdout=True)
def store_sentiment_analysis(host_count_data, conversations_with_sentiment_analysis):
    logger = prefect.context.get("logger")
    logger.info('connecting to database')
    connection = mysql.connector.connect(
        host=os.getenv('DATABASE_HOST', 'database'),
        user=os.getenv('DATABASE_USER', 'root'),
        password=os.getenv('DATABASE_PASSWORD', 'mariadb'),
        database=os.getenv('DATABASE_SCHEMA', 'twitter'),
    )
    cursor = connection.cursor()
    sql = 'insert into conversations(tweet_id, sentiment_score, hostname_id, insert_time) values (%s, %s, %s, %s)'
    logger.info('inserting tweet data')
    records = []
    for conversation in conversations_with_sentiment_analysis:
        logger.info(f'conversation: {conversation}')
        # format our data into a list of the data we want to insert
        records.append((
            conversation['id'],
            conversation['sentiment_score'],
            host_count_data[conversation['hostname']]['id'],
            prefect.context.date
        ))
    # executemany has an optimization for inserts where it converts multiple
    # individual insert statements into a multi-record insert
    cursor.executemany(sql, records)
    connection.commit()


# Let's plan this out:
# 1. get original tweets
# 2. give original tweets to conversation collector
# 3. perform sentiment analysis on conversations
# 4. extract hostname counts from original tweets
# 5. upsert hostname into database
# 6. save conversation id, run timestamp, and sentiment analysis result to database


with Flow("Twitter data") as flow:
    tweets = retrieve_tweets()
    tweets = extract_hostname_from_tweet.map(tweets)
    tweets = filter_tweets_without_hostnames(tweets)
    conversation_tweets_data = get_conversation_for_tweet.map(tweets)
    conversation_tweets_data = perform_sentiment_analysis.map(conversation_tweets_data)
    host_count_tweet = count_hostname(tweets)
    store_hostname(host_count_tweet)
    store_sentiment_analysis(host_count_tweet, conversation_tweets_data)


flow.run()
