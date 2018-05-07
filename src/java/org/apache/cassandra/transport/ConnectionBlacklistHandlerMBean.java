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
import java.util.List;

public interface ConnectionBlacklistHandlerMBean
{
    /**
     * Get all hostnames from which connections are actively blocked.
     *
     * @return List of hostnames (INetAddress)
     */
    public List<String> getBannedHostnames();

    /**
     * Add the given hostname to the list of blacklisted clients.
     */
    public void banHostname(String hostname);

    /**
     * Remove the given hostname from the list of blacklisted clients.
     */
    public void permitHostname(String hostname);
}
