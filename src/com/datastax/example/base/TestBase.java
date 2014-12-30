package com.datastax.example.base;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;

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
public class TestBase {

    final Logger logger = LoggerFactory.getLogger(TestBase.class);

    public Cluster cluster;
    public Session session;

    /**
     * Basic Cassandra connection initializer. Takes the cluster IPs and keyspace. Connects using TokenAwarePolicy.
     *
     * @param clusterIps
     * @param keySpace
     */
    public void initialize(String clusterIps, String keySpace) {
        cluster = Cluster
                .builder()
                .addContactPoint(clusterIps)
                .withRetryPolicy(DefaultRetryPolicy.INSTANCE)
                .withLoadBalancingPolicy(
                        new TokenAwarePolicy(new DCAwareRoundRobinPolicy()))
                .build();

        session = cluster.connect(keySpace);

        logger.info("Cassandra connection established to: " + clusterIps + " Keyspace: " + keySpace);

        schemaSetup();
    }

    /**
     * Tear down connection before closing.
     */
    public void cleanup() {
        session.close();
        cluster.close();

        logger.info("Cassandra connection closed");
    }

    public void schemaSetup() {

    }

    // Awesome random time generator.
    // Thanks to dasblinkenlight on stackoverflow.

    public Timestamp randomTimestamp() {
        return randomTimestamp("2013-01-01 00:00:00", "2014-01-01 00:00:00");
    }

    public Timestamp randomTimestamp(String fromDate, String toDate) {
        long offset = Timestamp.valueOf(fromDate).getTime();
        long end = Timestamp.valueOf(toDate).getTime();
        long diff = end - offset + 1;
        Timestamp rand = new Timestamp(offset + (long) (Math.random() * diff));
        return rand;
    }
}
