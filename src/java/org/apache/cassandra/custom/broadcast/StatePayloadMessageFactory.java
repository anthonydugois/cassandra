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

package org.apache.cassandra.custom.broadcast;

import javax.annotation.Nullable;

import org.apache.cassandra.custom.state.ClusterState;
import org.apache.cassandra.custom.state.EndpointState;
import org.apache.cassandra.custom.state.StatePayload;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.Verb;

public class StatePayloadMessageFactory implements MessageFactory<StatePayload>
{
    @Override
    @Nullable
    public Message<StatePayload> build()
    {
        EndpointState state = ClusterState.instance.updateLocalState();
        StatePayload payload = state.payload();

        if (payload.shouldSend())
        {
            return Message.out(Verb.STATE_PAYLOAD_MSG, state.payload());
        }

        return null;
    }
}
