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

package org.apache.cassandra.replica.state;

import java.nio.charset.StandardCharsets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.locator.Replica;

public class PendingRequest
{
    private static final Logger logger = LoggerFactory.getLogger(PendingRequest.class);

    private final SinglePartitionReadCommand command;

    private final String key;

    private boolean isPending = false;

    private Iterable<Replica> endpoints = null;

    public PendingRequest(SinglePartitionReadCommand command)
    {
        this.command = command;
        this.key = new String(command.partitionKey().getKey().array(), StandardCharsets.UTF_8);

        // logger.info("New pending request: " + key);
    }

    public void send(Iterable<Replica> endpoints)
    {
        this.isPending = true;
        this.endpoints = endpoints;

        LocalState.instance.incrementPendingRequests(endpoints);
    }

    public void receive()
    {
        if (this.isPending)
        {
            LocalState.instance.decrementPendingRequests(this.endpoints);

            this.isPending = false;
            this.endpoints = null;
        }
    }
}
