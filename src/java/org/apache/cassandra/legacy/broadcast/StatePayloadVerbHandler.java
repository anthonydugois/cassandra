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

package org.apache.cassandra.legacy.broadcast;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.legacy.state.ClusterState;
import org.apache.cassandra.legacy.state.EndpointState;
import org.apache.cassandra.legacy.state.StatePayload;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;

public class StatePayloadVerbHandler implements IVerbHandler<StatePayload>
{
    private static final Logger logger = LoggerFactory.getLogger(StatePayloadVerbHandler.class);

    public static final StatePayloadVerbHandler instance = new StatePayloadVerbHandler();

    @Override
    public void doVerb(Message<StatePayload> message) throws IOException
    {
        StatePayload payload = message.payload;
        InetAddressAndPort endpoint = message.from();

        EndpointState state = ClusterState.instance.getState(endpoint);

        state.setPendingReadCount(payload.getPendingReadCount());

        // logger.info("Received payload " + payload.getPendingReadCount() + " from " + endpoint);
    }
}