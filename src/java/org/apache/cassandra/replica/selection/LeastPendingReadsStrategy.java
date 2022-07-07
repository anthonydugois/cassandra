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

package org.apache.cassandra.replica.selection;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaCollection;
import org.apache.cassandra.replica.state.ClusterState;
import org.apache.cassandra.replica.state.LocalState;
import org.apache.cassandra.utils.FBUtilities;

public class LeastPendingReadsStrategy extends SelectionStrategy
{
    private static final Logger logger = LoggerFactory.getLogger(LeastPendingReadsStrategy.class);

    public LeastPendingReadsStrategy(IEndpointSnitch subsnitch, Map<String, String> parameters)
    {
        super(subsnitch, parameters);

        logger.info("Using " + this.getClass().getName() + " on top of " + subsnitch.getClass().getName());
    }

    private Map<Replica, Integer> getScores(Iterable<Replica> replicas)
    {
        Map<Replica, Integer> scores = new HashMap<>();

        for (Replica replica : replicas)
        {
            InetAddressAndPort endpoint = replica.endpoint();

            if (endpoint.equals(FBUtilities.getBroadcastAddressAndPort()))
            {
                scores.put(replica, LocalState.instance.getPendingReadCount());
            }
            else
            {
                scores.put(replica, ClusterState.instance.getState(endpoint).getPendingReadCount());
            }
        }

        return scores;
    }

    private void traceScores(Map<Replica, Integer> scores)
    {
        long epoch = System.currentTimeMillis();

        StringBuilder builder = new StringBuilder();

        for (Map.Entry<Replica, Integer> entry : scores.entrySet())
        {
            builder.append(',').append(entry.getKey().endpoint()).append(',').append(entry.getValue());
        }

        String line = epoch + builder.toString() + '\n';

        trace(line);
    }

    @Override
    public <C extends ReplicaCollection<? extends C>> C sortedByProximity(InetAddressAndPort address, C unsortedAddress)
    {
        Map<Replica, Integer> scores = getScores(unsortedAddress);

        if (shouldTrace())
        {
            traceScores(scores);
        }

        return unsortedAddress.sorted((r1, r2) -> compareEndpoints(address, r1, r2, scores));
    }

    @Override
    public int compareEndpoints(InetAddressAndPort target, Replica r1, Replica r2)
    {
        throw new UnsupportedOperationException();
    }

    public int compareEndpoints(InetAddressAndPort target, Replica r1, Replica r2, Map<Replica, Integer> scores)
    {
        Integer score1 = scores.getOrDefault(r1, 0);
        Integer score2 = scores.getOrDefault(r2, 0);

        if (score1.equals(score2))
        {
            return snitch.compareEndpoints(target, r1, r2);
        }

        return score1.compareTo(score2);
    }
}
