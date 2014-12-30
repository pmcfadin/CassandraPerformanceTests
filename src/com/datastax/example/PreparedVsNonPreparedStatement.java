package com.datastax.example;

import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
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
public class PreparedVsNonPreparedStatement extends TestBase {
    final Logger logger = LoggerFactory.getLogger(BatchVsExecuteAsync.class);

    static final MetricRegistry metrics = new MetricRegistry();
    private final Timer test1 = metrics.timer(MetricRegistry.name(PreparedVsNonPreparedStatement.class, "test1"));
    private final Timer test2 = metrics.timer(MetricRegistry.name(PreparedVsNonPreparedStatement.class, "test2"));

    public void allTests() {
        logger.info("Beginning all tests for PreparedVsNonPreparedStatement");

        test1();
        test2();

        logger.info("All tests for PreparedVsNonPreparedStatement complete");
    }

    // Non-prepared statement execute
    public void test1() {

        Random rnd = new Random();

        final CsvReporter reporter = CsvReporter.forRegistry(metrics)
                .formatFor(Locale.US)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build(new File("/Users/patrick/projects/"));

        logger.info("Beginning PreparedVsNonPreparedStatement:Test1");

        reporter.start(1, TimeUnit.SECONDS);

        //Insert 10000
        for (int i = 0; i < 1000000; i++) {

            String firstName = RandomStringUtils.randomAlphabetic(10);
            String lastName = RandomStringUtils.randomAlphabetic(10);
            String street = RandomStringUtils.randomAlphabetic(8);
            int post_code = rnd.nextInt(99999);
            int phone = rnd.nextInt(99999999);


            final Timer.Context context = test1.time();

            session.execute("insert into users (id, firstname, lastname, street, post_code, phone) VALUES (" + i + ", '" + firstName + "', '" + lastName + "', '" + street + "', " + post_code + ", " + phone + ");");

            context.stop();


        }

        logger.info("Completed PreparedVsNonPreparedStatement:Test1");

    }

    // Non-prepared statement execute
    public void test2() {

        Random rnd = new Random();
        final File file = new File("/Users/patrick/projects/PreparedVsNonPreparedStatement.csv");

        final CsvReporter reporter = CsvReporter.forRegistry(metrics)
                .formatFor(Locale.US)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build(new File("/Users/patrick/projects/"));

        logger.info("Beginning PreparedVsNonPreparedStatement:Test2");

        PreparedStatement userInsertStatement = session.prepare("insert into users (id, firstname, lastname, street, post_code, phone) VALUES (?, ?, ?, ?, ?, ?)");

        BoundStatement userInsert = new BoundStatement(userInsertStatement);

        reporter.start(1, TimeUnit.SECONDS);

        //Insert 10000
        for (int i = 1000000; i < 2000000; i++) {

            String firstName = RandomStringUtils.randomAlphabetic(10);
            String lastName = RandomStringUtils.randomAlphabetic(10);
            String street = RandomStringUtils.randomAlphabetic(8);
            int post_code = rnd.nextInt(99999);
            int phone = rnd.nextInt(99999999);


            final Timer.Context context = test2.time();

            session.execute(userInsert.bind(i, firstName, lastName, street, post_code, phone));

            context.stop();

        }

        logger.info("Completed PreparedVsNonPreparedStatement:Test2");

    }
}
