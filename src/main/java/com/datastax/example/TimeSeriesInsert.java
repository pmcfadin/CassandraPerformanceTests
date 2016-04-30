package com.datastax.example;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import com.datastax.driver.core.*;
import com.datastax.example.base.TestBase;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.concurrent.TimeUnit;

/**
 * Created by patrickmcfadin on 4/5/16.
 *
 * Runs a 24 insert on one table
 */
public class TimeSeriesInsert extends TestBase {

    final Logger logger = LoggerFactory.getLogger(TimeSeriesInsert.class);
    static final MetricRegistry metrics = new MetricRegistry();
    private final Meter readRequests = metrics.meter("read-requests");
    private final Meter readFull = metrics.meter(MetricRegistry.name(TimeSeriesInsert.class, "read-has-data"));
    private final Meter readEmpty = metrics.meter(MetricRegistry.name(TimeSeriesInsert.class, "read-no-data"));
    private final Meter readFailure = metrics.meter(MetricRegistry.name(TimeSeriesInsert.class, "read-failure"));
    private final Meter readSuccess = metrics.meter(MetricRegistry.name(TimeSeriesInsert.class, "read-success"));
    private final Timer readResponses = metrics.timer(MetricRegistry.name(TimeSeriesInsert.class, "reads"));

    static Graphite graphite;
    static GraphiteReporter reporter;


    public void load(String durationUnits, int duration, int recordCount) {
        logger.info("Beginning TimeSeriesInsert:load");

        SimpleDateFormat formatDayOnly = new SimpleDateFormat("yyyyMMdd");

        // Set test duration
        Calendar baseTime = getBaseTime(durationUnits, duration);

        long timeToStop = baseTime.getTimeInMillis();

        logger.info("TimeSeriesInsert: Test duration unit " + durationUnits + " for length " + duration);
        logger.info("TimeSeriesInsert: Test will end at " + baseTime.getTime().toString());
        logger.info("TimeSeriesInsert: Test will end with " + recordCount + " records inserted" );

        PreparedStatement recordInsertStatement = session.prepare("insert into timeseries (a, b, c) VALUES (?, ?, ?)");
        BoundStatement recordInsert = new BoundStatement(recordInsertStatement);

        long now = System.currentTimeMillis();

        long currentRecordCount = 0;

        // Set time series epoch date to Jan 1, 2000 at midnight
        Calendar timeseriesEpoch = new GregorianCalendar(2000,0,1,0,0,0);


        while(now < timeToStop){
            now = System.currentTimeMillis();

            // Store one day of second resolution data per partition
            for(int i = 0; i < 86400; i++) {

                String time = timeseriesEpoch.getTime().toString();

                // Use execute to make sure each record is inserted and the return code is correct.
                session.execute(recordInsert.bind(Integer.parseInt(formatDayOnly.format(timeseriesEpoch.getTime())), timeseriesEpoch.getTime(), RandomStringUtils.randomAlphabetic(1024)));
                currentRecordCount++;

                //Advance the epoch by one second
                timeseriesEpoch.add(Calendar.SECOND, 1);

                if((currentRecordCount % 100000) == 0){
                    logger.info("Records loaded: " + currentRecordCount);
                }
            }

            // Break at an even day boundry
            if(currentRecordCount > recordCount)
                break;
        }
        logger.info("Ending TimeSeriesInsert:load. Total records loaded: " + currentRecordCount);
    }

    public void randomRead(String durationUnits, int duration, int recordCount){

        logger.info("Beginning TimeSeriesInsert:randomRead");

        if (useGraphite) {
            graphite = new Graphite(new InetSocketAddress(graphiteHost, 2003));
            reporter = GraphiteReporter.forRegistry(metrics)
                    .prefixedWith(graphitePrefix)
                    .convertRatesTo(TimeUnit.SECONDS)
                    .convertDurationsTo(TimeUnit.MILLISECONDS)
                    .filter(MetricFilter.ALL)
                    .build(graphite);

            reporter.start(1, TimeUnit.MINUTES);
        }


        // a = YYYYMMDD
        // b = YYYYMMDD HH:mm:SS.s
        PreparedStatement recordSelectStatement = session.prepare("select c  from timeseries where a = ? AND b = ?");
        BoundStatement recordSelect = new BoundStatement(recordSelectStatement);

        ResultSet rs;

        // Set test duration
        Calendar baseTime = getBaseTime(durationUnits, duration);


        long timeToStop = baseTime.getTimeInMillis();
        long now = System.currentTimeMillis();

        long currentRecordCount = 0;

        SimpleDateFormat formatDayOnly = new SimpleDateFormat("yyyyMMdd");

        // Set time series epoch date to Jan 1, 2000 at midnight
        int year = randomNumberInRange(2000, 2010);
        int month = randomNumberInRange(1,12);
        int day = randomNumberInRange(1,28);

        Calendar timeSeriesRandomDate = new GregorianCalendar(year, month, day,0,0,0);

        int aValue = Integer.parseInt(formatDayOnly.format(timeSeriesRandomDate.getTime()));

        while(now < timeToStop) {
            now = System.currentTimeMillis();

            for (int i = 0; i < 86400; i++) {

                final Timer.Context context = readResponses.time();
                ResultSetFuture future = session.executeAsync(recordSelect.bind(aValue, timeSeriesRandomDate.getTime()));

                Futures.addCallback(future,
                        new FutureCallback<ResultSet>() {

                            public void onSuccess(ResultSet result) {
                                context.stop();
                                readSuccess.mark();
                                Row row = result.one();

                                if (result.getAvailableWithoutFetching() > 0) {
                                    readFull.mark();
                                } else {
                                    readEmpty.mark();
                                }

                            }

                            public void onFailure(Throwable t) {
                                context.stop();
                                readFailure.mark();
                            }
                        },
                        MoreExecutors.sameThreadExecutor()
                );

                //Advance the time by one second
                timeSeriesRandomDate.add(Calendar.SECOND, 1);
            }

        }
        //Give it a few seconds to wrap up
        try {
            Thread.sleep(5000);
        }catch (Exception e){
            logger.error(e.getMessage());
        }

        logger.info("Total reads " + readResponses.getCount());
        logger.info("Total successful reads " + readSuccess.getCount());
        logger.info("Total unsuccessful reads " + readFailure.getCount());
        logger.info("Total reads with data " + readFull.getCount());
        logger.info("Total reads without data " + readEmpty.getCount());
        logger.info("Read response 1 minute " + readResponses.getSnapshot().getMean());
        reporter.stop();
    }

    private int randomNumberInRange(int from, int to){

        return from + (int)(Math.random() * ((to - from) + 1));
    }
    private Calendar getBaseTime(String durationUnits, int duration){

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

        return baseTime;
    }
}


