/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.metrics;

import java.io.IOException;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.EmbeddedCassandraService;

import static junit.framework.Assert.assertEquals;

class MetricContainer
{

    private static final ClientRequestMetrics readMetrics = new ClientRequestMetrics("Read");
    private static final ClientRequestMetrics writeMetrics = new ClientRequestMetrics("Write");

    private static long readLocalRequests;
    private static long readRemoteRequests;

    private static long writeLocalRequests;
    private static long writeRemoteRequest;

    public MetricContainer()
    {
        readLocalRequests = readMetrics.localRequests.getCount();
        readRemoteRequests = readMetrics.remoteRequests.getCount();

        writeLocalRequests = writeMetrics.localRequests.getCount();
        writeRemoteRequest = writeMetrics.remoteRequests.getCount();
    }

    public long compareReadLocalRequest()
    {
        return readMetrics.localRequests.getCount() - readLocalRequests;
    }

    public long compareReadRemoteRequest()
    {
        return readMetrics.remoteRequests.getCount() - readRemoteRequests;
    }

    public long compareWriteLocalRequest()
    {
        return writeMetrics.localRequests.getCount() - writeLocalRequests;
    }

    public long compareWriteRemoteRequest()
    {
        return writeMetrics.remoteRequests.getCount() - writeRemoteRequest;
    }
}

@RunWith(OrderedJUnit4ClassRunner.class)
public class LocalRemoteRequestsMetricsTest extends SchemaLoader
{
    private static EmbeddedCassandraService cassandra;

    private static Cluster cluster;
    private static Session session;

    private static String KEYSPACE = "junit";
    private static final String TABLE = "localremoterequestsmetricstest";

    private static PreparedStatement writePS;
    private static PreparedStatement paxosPS;
    private static PreparedStatement readPS;
    private static PreparedStatement readRangePS;

    @BeforeClass()
    public static void setup() throws ConfigurationException, IOException
    {
        Schema.instance.clear();

        cassandra = new EmbeddedCassandraService();
        cassandra.start();

        cluster = Cluster.builder().addContactPoint("127.0.0.1").withPort(DatabaseDescriptor.getNativeTransportPort()).build();
        session = cluster.connect();

        session.execute("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE + " WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
        session.execute("USE " + KEYSPACE);
        session.execute("CREATE TABLE IF NOT EXISTS " + TABLE + " (id int, ord int, val text, PRIMARY KEY (id, ord));");

        writePS = session.prepare("INSERT INTO " + KEYSPACE + '.' + TABLE + " (id, ord, val) VALUES (?, ?, ?);");
        paxosPS = session.prepare("UPDATE " + KEYSPACE + '.' + TABLE + " set val=? WHERE id=? AND ord=?");
        readPS = session.prepare("SELECT * FROM " + KEYSPACE + '.' + TABLE + " WHERE id=?;");
        readRangePS = session.prepare("SELECT * FROM " + KEYSPACE + '.' + TABLE + " WHERE id=? AND ord>=? AND ord <= ?;");
    }

    private void executeWrite(int id, int ord, String val)
    {
        BoundStatement bs = writePS.bind(id, ord, val);
        session.execute(bs);
    }

    private void executePAXOS(int id, int ord, String new_val)
    {
        BoundStatement bs = paxosPS.bind(new_val, id, ord);
        session.execute(bs);
    }

    private void executeBatch(int distinctPartitions, int numClusteringKeys)
    {

        BatchStatement batch = new BatchStatement();

        for (int i=0; i<distinctPartitions; i++) {
            for (int y=0; y<numClusteringKeys; y++)
            {
                batch.add(writePS.bind(i, y, "aaaaaaaa"));
            }
        }

        session.execute(batch);
    }

    private void executeRead(int id)
    {
        BoundStatement bs = readPS.bind(id);
        session.execute(bs);
    }

    private void executeSlice(int id, int start_range, int end_range)
    {
        BoundStatement bs = readRangePS.bind(id, start_range, end_range);
        session.execute(bs);
    }

    @Test
    public void testWriteStatement()
    {
        MetricContainer metricContainer = new MetricContainer();

        executeWrite(1, 1, "aaaa");

        assertEquals(0, metricContainer.compareReadLocalRequest());
        assertEquals(0, metricContainer.compareReadRemoteRequest());

        assertEquals(1, metricContainer.compareWriteLocalRequest());
        assertEquals(0, metricContainer.compareWriteRemoteRequest());
    }

    @Test
    public void testPaxosStatement()
    {
        MetricContainer metricContainer = new MetricContainer();

        executePAXOS(2, 2, "aaaa");

        assertEquals(0, metricContainer.compareReadLocalRequest());
        assertEquals(0, metricContainer.compareReadRemoteRequest());

        assertEquals(1, metricContainer.compareWriteLocalRequest());
        assertEquals(0, metricContainer.compareWriteRemoteRequest());
    }

    @Test
    public void testBatchStatement()
    {
        MetricContainer metricContainer = new MetricContainer();

        executeBatch(10, 10);

        assertEquals(0, metricContainer.compareReadLocalRequest());
        assertEquals(0, metricContainer.compareReadRemoteRequest());

        assertEquals(10, metricContainer.compareWriteLocalRequest());
        assertEquals(0, metricContainer.compareWriteRemoteRequest());
    }

    @Test
    public void testReadStatement()
    {
        MetricContainer metricContainer = new MetricContainer();

        executeWrite(1, 1, "aaaa");
        executeRead(1);

        assertEquals(1, metricContainer.compareReadLocalRequest());
        assertEquals(0, metricContainer.compareReadRemoteRequest());

        assertEquals(1, metricContainer.compareWriteLocalRequest());
        assertEquals(0, metricContainer.compareWriteRemoteRequest());
    }

    @Test
    public void testRangeStatement()
    {
        MetricContainer metricContainer = new MetricContainer();

        executeBatch(1, 100);
        executeSlice(1, 0, 99);

        assertEquals(1, metricContainer.compareReadLocalRequest());
        assertEquals(0, metricContainer.compareReadRemoteRequest());

        assertEquals(1, metricContainer.compareWriteLocalRequest());
        assertEquals(0, metricContainer.compareWriteRemoteRequest());
    }
}

