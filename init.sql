create table hostname(
    id bigint auto_increment primary key,
    hostname varchar(256),
    counting int,
    index hostname(hostname)
);

