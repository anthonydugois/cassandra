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

package org.apache.cassandra.legacy.snitch;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaCollection;

public class LocalPendingStates
{
    public final static LocalPendingStates instance = new LocalPendingStates();

    private final ConcurrentMap<InetAddressAndPort, LocalPendingState> states = new ConcurrentHashMap<>();

    public ConcurrentMap<InetAddressAndPort, LocalPendingState> getStates()
    {
        return states;
    }

    public void addPendingKey(Iterable<InetAddressAndPort> endpoints, String key)
    {
        for (InetAddressAndPort endpoint : endpoints)
        {
            add(endpoint, key);
        }
    }

    public void removePendingKey(Iterable<InetAddressAndPort> endpoints, String key)
    {
        for (InetAddressAndPort endpoint : endpoints)
        {
            remove(endpoint, key);
        }
    }

    private LocalPendingState getState(InetAddressAndPort endpoint)
    {
        LocalPendingState state = states.get(endpoint);

        if (state == null)
        {
            LocalPendingState newState = new LocalPendingState();

            state = states.putIfAbsent(endpoint, newState);

            if (state == null)
            {
                state = newState;
            }
        }

        return state;
    }

    private void add(InetAddressAndPort endpoint, String key)
    {
        LocalPendingState state = getState(endpoint);

        state.add(key);
    }

    private void remove(InetAddressAndPort endpoint, String key)
    {
        LocalPendingState state = getState(endpoint);

        state.remove(key);
    }

    public <C extends ReplicaCollection<? extends C>> Map<InetAddressAndPort, Long> getPendingCounts(C replicas)
    {
        Map<InetAddressAndPort, Long> pendingCounts = new HashMap<>();

        for (Replica replica : replicas)
        {
            InetAddressAndPort endpoint = replica.endpoint();
            LocalPendingState state = states.get(endpoint);

            if (state == null)
            {
                pendingCounts.put(endpoint, 0L);
            }
            else
            {
                pendingCounts.put(endpoint, state.getPendingCount());
            }
        }

        return pendingCounts;
    }

    public Map<InetAddressAndPort, Long> getFinishTimes()
    {
        Map<InetAddressAndPort, Long> finishTimes = new HashMap<>();

        for (Map.Entry<InetAddressAndPort, LocalPendingState> entry : states.entrySet())
        {
            InetAddressAndPort endpoint = entry.getKey();
            LocalPendingState state = entry.getValue();

            finishTimes.put(endpoint, state.getExpectedFinishTime());
        }

        return finishTimes;
    }
}
