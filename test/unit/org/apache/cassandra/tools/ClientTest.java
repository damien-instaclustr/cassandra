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

package org.apache.cassandra.tools;

import java.io.IOException;

import org.apache.commons.lang3.ArrayUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.service.EmbeddedCassandraService;
import com.datastax.driver.core.exceptions.NoHostAvailableException;

public class ClientTest extends ToolsTester
{
    private static EmbeddedCassandraService cassandra;

    private static Cluster cluster;
    private static NodeProbe probe;
    private static final int JMX_PORT = 7188;

    @BeforeClass()
    public static void setup() throws ConfigurationException, IOException
    {
        System.setProperty("cassandra.jmx.local.port", String.valueOf(JMX_PORT));

        SchemaLoader.prepareServer();
        cassandra = new EmbeddedCassandraService();
        cassandra.start();

        probe = new NodeProbe("127.0.0.1", JMX_PORT);
    }

    @AfterClass
    public static void teardown() throws IOException
    {
        probe.close();
        cassandra.stop();
    }

    private String[] constructParamaterArray(final String command, final String... commandParams)
    {
        String[] baseCommandLine = {"-p", String.valueOf(JMX_PORT), command};
        return ArrayUtils.addAll(baseCommandLine, commandParams);
    }

    @Test
    public void testDisableEnableClient()
    {
        cluster = Cluster.builder().addContactPoint("127.0.0.1").withPort(DatabaseDescriptor.getNativeTransportPort()).build();
        cluster.connect();

        assert(1 <= (int)probe.getClientMetric("connectedNativeClients"));

        runTool(0, "org.apache.cassandra.tools.NodeTool",
                constructParamaterArray("disableclient", "localhost"));

        assert(0 == (int)probe.getClientMetric("connectedNativeClients"));

        try
        {
            cluster = Cluster.builder().addContactPoint("127.0.0.1").withPort(DatabaseDescriptor.getNativeTransportPort()).build();
            cluster.connect();
        }
        catch (NoHostAvailableException e) {
            // NoHostAvailableException is expected
        }
        assert(0 == (int)probe.getClientMetric("connectedNativeClients"));

        runTool(0, "org.apache.cassandra.tools.NodeTool",
                constructParamaterArray("enableclient", "127.0.0.1"));

        cluster = Cluster.builder().addContactPoint("127.0.0.1").withPort(DatabaseDescriptor.getNativeTransportPort()).build();
        cluster.connect();
        assert(1 <= (int)probe.getClientMetric("connectedNativeClients"));
    }
}
