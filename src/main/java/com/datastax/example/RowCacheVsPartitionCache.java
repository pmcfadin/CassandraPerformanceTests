package com.datastax.example;

import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.example.base.TestBase;
import org.apache.commons.lang.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/*

Copyright 2014 Patrick McFadin

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

*/
public class RowCacheVsPartitionCache extends TestBase {
    final Logger logger = LoggerFactory.getLogger(RowCacheVsPartitionCache.class);
    static final MetricRegistry metrics = new MetricRegistry();
    private final Timer test1 = metrics.timer(MetricRegistry.name(RowCacheVsPartitionCache.class, "test1"));
    private final Timer test2 = metrics.timer(MetricRegistry.name(RowCacheVsPartitionCache.class, "test2"));

    public void allTests() {
        logger.info("Beginning all tests for RowCacheVsPartitionCache");

        //load();
        test1();
        test2();

        logger.info("All tests for RowCacheVsPartitionCache complete");
    }

    public void load() {
        Random rnd = new Random();

        logger.info("Beginning RowCacheVsPartitionCache:load");

        PreparedStatement userSearchInsertStatement = session.prepare("insert into user_search_history (id, search_time, search_text, search_results) VALUES (?, ?, ?, ?)");
        BoundStatement userSearchInsert = new BoundStatement(userSearchInsertStatement);

        PreparedStatement userSearchCachedInsertStatement = session.prepare("insert into user_search_history_with_cache (id, search_time, search_text, search_results) VALUES (?, ?, ?, ?)");
        BoundStatement userSearchCachedInsert = new BoundStatement(userSearchCachedInsertStatement);

        //Insert times for 1000 users
        for (int i = 0; i < 1000000; i++) {

            // Create a random number of search history records to create
            Random rand = new Random();

            // We'll need a minimum of 10 and a max of 100 search records
            int randomRecordCount = rand.nextInt(90) + 10;

            for (int j = 0; j < randomRecordCount; j++){

                session.executeAsync(userSearchInsert.bind(i, randomTimestamp(), RandomStringUtils.randomAlphabetic(10), rnd.nextInt(99999)));
                session.executeAsync(userSearchCachedInsert.bind(i, randomTimestamp(), RandomStringUtils.randomAlphabetic(10), rnd.nextInt(99999)));

            }

        }

        logger.info("Completed RowCacheVsPartitionCache:load");
    }

    public void test1() {
        Random rnd = new Random();

        final CsvReporter reporter = CsvReporter.forRegistry(metrics)
                .formatFor(Locale.US)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build(new File("/Users/patrickmcfadin/projects/CassandraPerformanceTests/logs"));

        logger.info("Beginning RowCacheVsPartitionCache:Test1 - No Cache");

        PreparedStatement userSearchSelectStatement = session.prepare("SELECT * FROM user_search_history WHERE id = ? LIMIT 10");

        BoundStatement userSearchSelect = new BoundStatement(userSearchSelectStatement);
        reporter.start(1, TimeUnit.SECONDS);

        int id;

        //Insert 10000
        for (int i = 0; i < 1000000; i++) {

            id = rnd.nextInt(999999);

            final Timer.Context context = test1.time();

            ResultSet rs = session.execute(userSearchSelect.bind(id));

            context.stop();


        }
        reporter.stop();

        logger.info("Completed RowCacheVsPartitionCache:Test1 - No Cache");
    }

    public void test2() {
        Random rnd = new Random();

        final CsvReporter reporter = CsvReporter.forRegistry(metrics)
                .formatFor(Locale.US)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build(new File("/Users/patrickmcfadin/projects/CassandraPerformanceTests/logs"));

        logger.info("Beginning RowCacheVsPartitionCache:Test2 - Partition Cache");

        PreparedStatement userSearchSelectStatement = session.prepare("SELECT * FROM user_search_history_with_cache WHERE id = ? LIMIT 10");

        BoundStatement userSearchSelect = new BoundStatement(userSearchSelectStatement);
        reporter.start(1, TimeUnit.SECONDS);

        int id;

        //Insert 10000
        for (int i = 0; i < 1000000; i++) {

            id = rnd.nextInt(9999);

            final Timer.Context context = test2.time();

            ResultSet rs = session.execute(userSearchSelect.bind(id));

            context.stop();


        }
        reporter.stop();
        logger.info("Completed RowCacheVsPartitionCache:Test2 - Partition Cache");
    }
}
