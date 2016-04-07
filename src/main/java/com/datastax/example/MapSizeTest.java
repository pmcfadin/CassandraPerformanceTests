package com.datastax.example;

import com.codahale.metrics.*;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.example.base.TestBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Locale;
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
public class MapSizeTest extends TestBase {

    static final MetricRegistry metrics = new MetricRegistry();
    private final Timer requestLatency = metrics.timer(MetricRegistry.name(MapSizeTest.class, "requestLatency"));

    final Logger logger = LoggerFactory.getLogger(MapSizeTest.class);


    @Override
    public void schemaSetup() {
        //Check for schema

        logger.info("Checking for MapSizeTest schema");

        ResultSet result = session.execute("select keyspace_name, columnfamily_name from system.schema_columnfamilies where keyspace_name = 'perf_test' and columnfamily_name = 'maptest';");

        if (result.one() == null) {
            logger.info("MapSizeTest schema does not exist. Creating...");

            result = session.execute("CREATE TABLE maptest (\n" +
                    "    id int PRIMARY KEY,\n" +
                    "    textMap map<text,text>,\n" +
                    "    intMap map<int,int>\n" +
                    ");");

            for(Row row: result){
                logger.info(row.toString());
            }
            logger.info("MapSizeTest schema created");

        } else {
            logger.info("MapSizeTest schema exists");
        }
    }

    public void allTests() {
        logger.info("Beginning all tests for MapSizeTest");

        test1();

        logger.info("All tests for MapSizeTest complete");
    }

    public void test1() {

        final CsvReporter reporter = CsvReporter.forRegistry(metrics)
                .formatFor(Locale.US)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build(new File("/Users/patrickmcfadin/projects/CassandraPerformanceTests/logs"));

        logger.info("Beginning MapSizeTest:Test1");

        int key = 0;

        reporter.start(1, TimeUnit.SECONDS);

        //Insert 100 items at a time, up to 64k items
        for (int i = 0; i < 640; i++) {

            for (int j = 0; j < 100; j++) {

                final Timer.Context context = requestLatency.time();
                session.execute("update maptest set intMap = {" + key + ":" + key + "} where id = 0");
                context.stop();

                key++;
            }
        }

        logger.info("Completed MapSizeTest:Test1");
    }
}
