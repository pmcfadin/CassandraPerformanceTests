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
import java.util.GregorianCalendar;
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

    public void load(String durationUnits, int duration) {
        logger.info("Beginning TimeSeriesInsert:load");

        // Set test duration
        Calendar baseTime = Calendar.getInstance();

        switch(durationUnits){
            case "SECOND": baseTime.add(Calendar.SECOND, duration);
                break;
            case "MINUTE": baseTime.add(Calendar.MINUTE, duration);
                break;
            case "HOUR": baseTime.add(Calendar.HOUR, duration);
                break;

            //If no usable input, run for 30 seconds
            default: baseTime.add(Calendar.SECOND, 30);

        }

        long timeToStop = baseTime.getTimeInMillis();

        logger.info("TimeSeriesInsert: Test duration unit " + durationUnits + " for length " + duration);
        logger.info("TimeSeriesInsert: Test will end at " + baseTime.getTime().toString());

        PreparedStatement recordInsertStatement = session.prepare("insert into timeseries (a, b, c) VALUES (?, ?, ?)");
        BoundStatement recordInsert = new BoundStatement(recordInsertStatement);

        long now = System.currentTimeMillis();

        long recordCount = 0;
        UUID id;

        // Set time series epoch date to Jan 1, 2000 at midnight
        Calendar timeseriesEpoch = new GregorianCalendar(2000,1,1,0,0,0);


        while(now < timeToStop){
            now = System.currentTimeMillis();
            id = UUID.randomUUID();

            // Store one day of second resolution data per partition
            for(int i = 0; i < 86400; i++) {

                // Use execute to make sure each record is inserted and the return code is correct.
                session.execute(recordInsert.bind(id, new java.sql.Date(timeseriesEpoch.getTimeInMillis()), RandomStringUtils.randomAlphabetic(1024)));
                recordCount++;

                //Advance the epoch by one second
                timeseriesEpoch.add(Calendar.SECOND, 1);


                if((recordCount % 100000) == 0){
                    logger.info("Records loaded: " + recordCount);
                }
            }
        }
        logger.info("Ending TimeSeriesInsert:load. Total records loaded: " + recordCount);
    }
}


