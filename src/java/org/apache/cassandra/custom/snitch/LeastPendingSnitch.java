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

package org.apache.cassandra.custom.snitch;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.custom.state.ClusterState;
import org.apache.cassandra.custom.state.EndpointState;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaCollection;
import org.apache.cassandra.utils.FBUtilities;

public class LeastPendingSnitch extends SelectionSnitch
{
    private static final Logger logger = LoggerFactory.getLogger(LeastPendingSnitch.class);

    public LeastPendingSnitch(IEndpointSnitch subsnitch, Map<String, String> parameters)
    {
        super(subsnitch, parameters);

        logger.info("Using " + this.getClass().getName() + " on top of " + subsnitch.getClass().getName());
    }

    private Map<Replica, Long> getScores(Iterable<Replica> replicas)
    {
        InetAddressAndPort local = FBUtilities.getBroadcastAddressAndPort();

        Map<Replica, Long> scores = new HashMap<>();

        for (Replica replica : replicas)
        {
            EndpointState state;

            InetAddressAndPort endpoint = replica.endpoint();

            if (endpoint == local)
            {
                state = ClusterState.instance.updateLocalState();
            }
            else
            {
                state = ClusterState.instance.getState(endpoint);
            }

            scores.put(replica, (long) state.getPendingReadCount());
        }

        return scores;
    }

    @Override
    public <C extends ReplicaCollection<? extends C>> C sortedByProximity(InetAddressAndPort address, C unsortedAddress)
    {
        Map<Replica, Long> scores = getScores(unsortedAddress);

        long epoch = System.currentTimeMillis();

        StringBuilder builder = new StringBuilder();

        for (Map.Entry<Replica, Long> entry : scores.entrySet())
        {
            builder.append(',').append(entry.getKey().endpoint()).append(',').append(entry.getValue());
        }

        String line = epoch + builder.toString() + '\n';

        try
        {
            trace(line);
        }
        catch (IOException exception)
        {
            exception.printStackTrace();
        }

        return unsortedAddress.sorted((r1, r2) -> compareEndpoints(address, r1, r2, scores));
    }

    @Override
    public int compareEndpoints(InetAddressAndPort target, Replica r1, Replica r2)
    {
        throw new UnsupportedOperationException();
    }

    public int compareEndpoints(InetAddressAndPort target, Replica r1, Replica r2, Map<Replica, Long> states)
    {
        Long score1 = states.getOrDefault(r1, 0L);
        Long score2 = states.getOrDefault(r2, 0L);

        if (score1.equals(score2))
        {
            return subsnitch.compareEndpoints(target, r1, r2);
        }

        return score1.compareTo(score2);
    }
}
