DROP KEYSPACE IF EXISTS perf_test;
CREATE KEYSPACE perf_test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

use perf_test;

CREATE TABLE maptest (
    id int PRIMARY KEY,
    textMap map<text,text>,
    intMap map<int,int>
);

