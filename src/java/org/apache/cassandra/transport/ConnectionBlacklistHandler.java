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

package org.apache.cassandra.transport;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import org.apache.cassandra.transport.Server.ConnectionTracker;

@ChannelHandler.Sharable
public class ConnectionBlacklistHandler extends ChannelInboundHandlerAdapter implements ConnectionBlacklistHandlerMBean
{
    public static final String MBEAN_NAME = "org.apache.cassandra.db:type=Connection";

    private static final Logger logger = LoggerFactory.getLogger(ConnectionLimitHandler.class);

    private static final List<InetAddress> blacklist = new ArrayList<>();
    private static ConnectionTracker connectionTracker;

    public static final ConnectionBlacklistHandler instance = new ConnectionBlacklistHandler();

    static
    {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try
        {
            mbs.registerMBean(instance, new ObjectName(MBEAN_NAME));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public void setConnectionTracker(ConnectionTracker ct) { connectionTracker = ct; }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception
    {
        for (InetAddress addr : blacklist)
        {
            if (addr.equals(((InetSocketAddress)ctx.channel().remoteAddress()).getAddress()))
            {
                logger.info("Blocking client connection from {}", addr.toString());
                ctx.close();
                return;
            }
        }

        ctx.fireChannelActive();
    }

    public List<String> getBannedHostnames()
    {
        List<String> bannedHostnames = new ArrayList<>();
        for (InetAddress addr : blacklist)
        {
            bannedHostnames.add(addr.toString());
        }
        return bannedHostnames;
    }

    public void banHostname(String hostname)
    {
        InetAddress addr;

        try {
            addr = InetAddress.getByName(hostname);
        } catch (UnknownHostException var6) {
            return;
        }

        if (!blacklist.contains(addr))
            blacklist.add(addr);

        if (connectionTracker != null)
        {
            for (Channel c : connectionTracker.allChannels)
            {
                Connection connection = c.attr(Connection.attributeKey).get();
                if (connection instanceof ServerConnection)
                {
                    ServerConnection conn = (ServerConnection) connection;
                    if (conn.getClientState().getRemoteAddress().getAddress().equals(addr))
                    {
                        c.close().awaitUninterruptibly();
                    }
                }
            }
        }
    }

    public void permitHostname(String hostname)
    {
        InetAddress addr;

        try {
            addr = InetAddress.getByName(hostname);
        } catch (UnknownHostException var6) {
            return;
        }

        if (blacklist.contains(addr))
            blacklist.remove(addr);
    }

}
