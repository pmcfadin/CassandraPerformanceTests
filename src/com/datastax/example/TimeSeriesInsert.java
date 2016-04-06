package com.datastax.example;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.example.base.TestBase;
import org.apache.commons.lang.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Calendar;
import java.util.UUID;

/**
 * Created by patrickmcfadin on 4/5/16.
 *
 * Runs a 24 insert on one table
 */
public class TimeSeriesInsert extends TestBase {

    final Logger logger = LoggerFactory.getLogger(GetAndSetTest.class);
    static final MetricRegistry metrics = new MetricRegistry();
    private final Timer load = metrics.timer(MetricRegistry.name(TimeSeriesInsert.class, "load"));

    public void load() {
        logger.info("Beginning TimeSeriesInsert:load");

        Calendar baseTime = Calendar.getInstance();
        baseTime.add(Calendar.HOUR, 24);

        long timeToStop = baseTime.getTimeInMillis();


        PreparedStatement recordInsertStatement = session.prepare("insert into timeseries (a, b, c) VALUES (?, ?, ?)");
        BoundStatement recordInsert = new BoundStatement(recordInsertStatement);

        long now = System.currentTimeMillis();
        long recordCount = 0;
        UUID id;

        while(now < timeToStop){
            now = System.currentTimeMillis();
            id = UUID.randomUUID();

            // Store one minute of data per partition
            for(int i = 0; i < 60000; i++) {
                // Use execute to make sure each record is inserted and the return code is correct.
                session.execute(recordInsert.bind(id, new java.sql.Date(System.currentTimeMillis()), RandomStringUtils.randomAlphabetic(1024)));
            }
            if(recordCount%10000 == 0){
                logger.info("Records loaded: " + recordCount);
            }
        }
        logger.info("Ending TimeSeriesInsert:load. Total records loaded: " + recordCount);
    }
}


