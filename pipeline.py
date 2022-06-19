import json
import os
import urllib.parse

import prefect
from prefect import task, Flow
import requests
import mysql.connector


@task(log_stdout=True)
def retrieve_tweets():
    logger = prefect.context.get("logger")
    auth_token = os.getenv('TWITTER_BEARER_TOKEN')
    logger.info('about to request tweets')
    response = requests.get(
        'https://api.twitter.com/2/tweets/search/recent',
        params={
            'query': '"data science" has:links',
            'tweet.fields': 'entities',
            'max_results': '100',
        },
        headers={
            'Authorization': f'Bearer {auth_token}',
        },
    )
    results = response.json()
    logger.info(f'data:{json.dumps(results)}')
    logger.info(f'got {len(results["data"])} tweets')
    return results



@task(log_stdout=True)
def count_hostname(response):
    logger = prefect.context.get("logger")
    logger.info('classifying tweet data with links')
    count_tweets_for_hosts = dict()
    for tweet in response['data']:
        if 'urls' in tweet['entities']:
            for url in tweet['entities']['urls']:
                if 'unwound_url' in url:
                    tweet_host_name = urllib.parse.urlparse(url['unwound_url']).hostname
                else:
                    tweet_host_name = urllib.parse.urlparse(url['expanded_url']).hostname
                if tweet_host_name not in count_tweets_for_hosts:
                    count_tweets_for_hosts[tweet_host_name] = {
                        'count': 1
                    }
                    # {
                    #   'google.com': {
                    #       'count': 1
                    #    },
                    #   'netflix.com': {
                    #       'count': 1
                    #    }
                    #  }
                else:
                    count_tweets_for_hosts[tweet_host_name]['count'] += 1
    logger.info(f'added {len(count_tweets_for_hosts)} as a dictionary')
    logger.info(f'added {count_tweets_for_hosts} as a dictionary')
    response["counts_tweets_for_hosts"] = count_tweets_for_hosts
    logger.info(f'{response}')
    return response


@task(log_stdout=True)
def transform_hostname_data(response):
    logger = prefect.context.get("logger")
    logger.info('transforming hostname-added tweet data')
    data = [[k1, response['counts_tweets_for_hosts'][k1]['count']] for k1 in response['counts_tweets_for_hosts']]
    logger.info(f'got {data}')
    logger.info(f'{data}')
    return data


@task(log_stdout=True)
def store_data(records):
    logger = prefect.context.get("logger")
    logger.info('connecting to database')
    connection = mysql.connector.connect(
        host="database",
        user="root",
        password="mariadb",
        database="twitter"
    )
    cursor = connection.cursor()
    sql = 'insert into tweet(id, content) values (%s, %s)'
    logger.info('inserting tweet data')
    # executemany has an optimization for inserts where it converts multiple
    # individual insert statements into a multi-record insert
    cursor.executemany(sql, records)
    connection.commit()


with Flow("Twitter data") as flow:
    tweets = retrieve_tweets()
    host_count_tweet = count_hostname(tweets)
    host_count_tweets = transform_hostname_data(host_count_tweet)
    # store_hostname(host_count_tweets)


flow.run()
