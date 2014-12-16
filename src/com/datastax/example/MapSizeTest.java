package com.datastax.example;

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
public class MapSizeTest extends TestBase {

    final Logger logger = LoggerFactory.getLogger(MapSizeTest.class);

    public void allTests() {
        logger.info("Beginning all tests for MapSizeTest");

        test1();

        logger.info("All tests for MapSizeTest complete");
    }

    public void test1() {

        logger.info("Beginning MapSizeTest:Test1");

        int key = 0;

        //Insert 100 items at a time, up to 64k items
        for (int i = 0; i < 640; i++) {
            long startTime = System.nanoTime();

            for (int j = 0; j < 100; j++) {

                session.execute("update maptest set intMap = {" + key + ":" + key + "} where id = 0");

                key++;
            }
            logger.info("Total time for 100 updates: " + (System.nanoTime() - startTime));

        }
        logger.info("Completed MapSizeTest:Test1");
    }
}
