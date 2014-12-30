package com.datastax.example;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.datastax.example.base.TestBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class BatchVsExecuteAsync extends TestBase {
    final Logger logger = LoggerFactory.getLogger(BatchVsExecuteAsync.class);
    static final MetricRegistry metrics = new MetricRegistry();
    private final Timer test1 = metrics.timer(MetricRegistry.name(PreparedVsNonPreparedStatement.class, "test1"));
    private final Timer test2 = metrics.timer(MetricRegistry.name(PreparedVsNonPreparedStatement.class, "test2"));

    public void allTests() {
        logger.info("Beginning all tests for PreparedVsNonPreparedStatement");

        //test1();
        //test2();

        logger.info("All tests for PreparedVsNonPreparedStatement complete");
    }
}
