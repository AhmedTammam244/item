-- create prod database
create database prod;

-- create category table
drop table if exists category;
create table category(category_id smallint primary key, parent_id smallint);


-- create event table
drop table if exists event;
create table event
(
    created_at     timestamp,
    visitorid     integer,
    event         varchar(20),
    itemid        integer,
    transactionid integer
);


-- create item table
drop table if exists item;
create table item
(
    created_at     timestamp,
    itemid       integer,
    property     varchar(20),
    value        varchar(2000),
    categoryid integer,
    available smallint
);


