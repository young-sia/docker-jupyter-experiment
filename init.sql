create table tweet(
    id bigint auto_increment primary key,
    id_tweet varchar(64),
    api_tweet varchar(128),
    content varchar(512)
);

create table retweet(
    id bigint auto_increment primary key,
    id_tweet varchar(64),
    user_id varchar(64),
    count_retweets int,
    index retweet_id_tweet(id_tweet)
);

create table tweet_link(
    id bigint auto_increment primary key,
    id_tweet varchar(64),
    api_tweet varchar(512),
    link varchar(256),
    sentiment int,
    index tweet_link_link(link),
    index tweet_link_id_tweet(id_tweet)
);

create table link_analyze(
    id bigint auto_increment primary key,
    link varchar(256),
    counting int,
    sentiment_count int,
    index link_analyze_link(link)
);

