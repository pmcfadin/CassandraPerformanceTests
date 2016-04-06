DROP KEYSPACE IF EXISTS perf_test;
CREATE KEYSPACE perf_test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

use perf_test;

CREATE TABLE maptest (
    id int PRIMARY KEY,
    textMap map<text,text>,
    intMap map<int,int>
);

CREATE TABLE users (
    id int PRIMARY KEY,
    firstName text,
    lastName text,
    street text,
    post_code int,
    phone int
);

// insert into users (id, firstname, lastname, street, post_code, phone) VALUES (?, ?, ?, ?, ?, ?)

CREATE TABLE user_search_history (
    id int,
    search_time timestamp,
    search_text text,
    search_results int,
    PRIMARY KEY (id, search_time)
) WITH CLUSTERING ORDER BY (search_time DESC);

// insert into user_search_history (id, search_time, search_text, search_results) VALUES (?, ?, ?, ?)

CREATE TABLE user_search_history_with_cache (
    id int,
    search_time timestamp,
    search_text text,
    search_results int,
    PRIMARY KEY (id, search_time)
) WITH CLUSTERING ORDER BY (search_time DESC)
AND caching = { 'keys' : 'ALL', 'rows_per_partition' : '20' };

CREATE TABLE static_records (
    a uuid,
    b text,
    c text,
    PRIMARY KEY (a)
);

CREATE TABLE lookup_table (
    a int,
    b uuid,
    c text,
    PRIMARY KEY (a,b)
);

CREATE TABLE timeseries (
    a uuid,
    b timestamp,
    c text,
   PRIMARY KEY (a, b)
) with clustering order by (b desc);

//TWCS
ALTER TABLE timeseries with
compaction= {
'compaction_window_unit': 'HOURS',
'compaction_window_size': '1',
'class':'TimeWindowCompactionStrategy'
};

//DTCS
AND
    compaction = {'class': 'DateTieredCompactionStrategy',
    'timestamp_resolution':'seconds', 'base_time_seconds':'225',
    'max_window_size_seconds': 3600,  'max_sstable_age_days':'365'};
