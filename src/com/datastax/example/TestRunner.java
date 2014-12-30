package com.datastax.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

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

public class TestRunner {

    static final Logger logger = LoggerFactory.getLogger(TestRunner.class);



    public static void main(String[] args) {

        logger.info("Test runner started");

        Properties prop = new Properties();

        try {
            prop.load(new FileInputStream("resources/test.properties"));

        } catch (FileNotFoundException e) {
           logger.error("test.properties not found");
           System.exit(1);
        } catch (IOException e) {
            logger.error(e.getMessage());
            System.exit(1);
        }

        BatchVsExecuteAsync batchTest = new BatchVsExecuteAsync();
        MapSizeTest mapTest = new MapSizeTest();
        PreparedVsNonPreparedStatement psTest = new PreparedVsNonPreparedStatement();
        RowCacheVsPartitionCache cacheTest = new RowCacheVsPartitionCache();

        //batchTest.initialize(prop.getProperty("cluster_ips"), prop.getProperty("keyspace"));
       // batchTest.cleanup();

        //mapTest.initialize(prop.getProperty("cluster_ips"), prop.getProperty("keyspace"));
        //mapTest.allTests();
        //mapTest.cleanup();

        //psTest.initialize(prop.getProperty("cluster_ips"), prop.getProperty("keyspace"));
        //psTest.allTests();
        //psTest.cleanup();

        cacheTest.initialize(prop.getProperty("cluster_ips"), prop.getProperty("keyspace"));
        cacheTest.allTests();
        cacheTest.cleanup();
    }
}
