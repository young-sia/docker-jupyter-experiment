create table hostname(
    id bigint auto_increment primary key,
    hostname varchar(256),
    counting int,
    index hostname(hostname)
);

create table conversations(
    tweet_id bigint primary key,
    sentiment_score varchar(8),
    hostname_id varchar(255),
    insert_time datetime,
    index hostname_id(hostname_id),
    index insert_time(insert_time)
);
