package com.datastax.example;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.datastax.driver.core.*;
import com.datastax.example.base.TestBase;
import org.apache.commons.lang.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;

import java.util.HashMap;
import java.util.UUID;

/**
 * Test to simulate getting a record list from one partition and using the result to set a value in multiple partitions.
 *
 */
public class GetAndSetTest extends TestBase {
    final Logger logger = LoggerFactory.getLogger(GetAndSetTest.class);
    static final MetricRegistry metrics = new MetricRegistry();
    private final Timer test1 = metrics.timer(MetricRegistry.name(GetAndSetTest.class, "test1"));

    static final int recordNumbers[] = {1000, 5000, 10000, 20000, 30000, 40000, 50000};

    static int insertSuccess = 0;
    static int insertFailure = 0;
    static int remainingFutures = 0;

    static {
        schema = new HashMap<String, String>();
        schema.put("static_records","CREATE TABLE static_records (\n" +
                "    a uuid,\n" +
                "    b text,\n" +
                "    c text,\n" +
                "    PRIMARY KEY (a)\n" +
                ");");

        schema.put("lookup_table","CREATE TABLE lookup_table (\n" +
                "    a int,\n" +
                "    b uuid,\n" +
                "    c text,\n" +
                "    PRIMARY KEY (a,b)\n" +
                ");");
    }

    public void load() {

        logger.info("Beginning GetAndSetTest:load");

        PreparedStatement recordInsertStatement = session.prepare("insert into static_records (a, b) VALUES (?, ?)");
        BoundStatement recordInsert = new BoundStatement(recordInsertStatement);

        PreparedStatement lookupInsertStatement = session.prepare("insert into lookup_table (a, b) VALUES (?, ?)");
        BoundStatement lookupInsert = new BoundStatement(lookupInsertStatement);

        UUID staticRecordId;
        long time;

        logger.info("Truncating previous GetAndSetTest records");

        // Clean out the existing records
        session.execute("truncate static_records");
        session.execute("truncate lookup_table");

        // Iterate through each record count number
        for (int recordNumber : recordNumbers) {

            logger.info("Loading " + recordNumber + " GetAndSetTest records");
            time = System.currentTimeMillis();

            for (int i = 0; i < recordNumber; i++) {

                staticRecordId = UUID.randomUUID();

                // Use execute to make sure each record is inserted and the return code is correct.
                session.execute(recordInsert.bind(staticRecordId, RandomStringUtils.randomAlphabetic(10)));
                session.execute(lookupInsert.bind(recordNumber, staticRecordId));
            }

            logger.info("Total time to load " + recordNumber + " static records: " + (System.currentTimeMillis() - time));
        }
        logger.info("Finishing GetAndSetTest:load");

    }

    public void test1() {

        logger.info("Beginning GetAndSetTest:test1");

        PreparedStatement recordUpdateStatement = session.prepare("update static_records set c = ? where a = ?");
        BoundStatement recordUpdate = new BoundStatement(recordUpdateStatement);

        long time = System.currentTimeMillis();
        int records;

        // Iterate through each record count number
        for (int recordNumber : recordNumbers) {
            records = 0;
            insertFailure = 0;
            insertSuccess = 0;
            remainingFutures = 0;

            logger.info("Reading records for " + recordNumber + " record test");

            ResultSet rs = session.execute("select b from lookup_table where a = " + recordNumber);

            for (Row row : rs) {

                ResultSetFuture future = session.executeAsync(recordUpdate.bind("Updated", row.getUUID("b")));
                records++;
                remainingFutures++;
                Futures.addCallback(future,
                        new FutureCallback<ResultSet>() {

                            public void onSuccess(ResultSet result) {
                                insertSuccess++;
                                remainingFutures--;
                            }

                            public void onFailure(Throwable t) {
                                insertFailure++;
                                remainingFutures--;
                            }
                        },
                        MoreExecutors.sameThreadExecutor()
                );
            }

            logger.info("Total time to update " + records + " records: " + (System.currentTimeMillis() - time));

            logger.info("Total records succeeded: " + insertSuccess + " failed: " + insertFailure);
        }

        logger.info("Finishing GetAndSetTest:load");

    }
}
