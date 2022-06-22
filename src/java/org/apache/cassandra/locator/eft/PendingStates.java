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

package org.apache.cassandra.locator.eft;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.cassandra.locator.InetAddressAndPort;

public class PendingStates
{
    public final static PendingStates instance = new PendingStates();

    private final ConcurrentMap<InetAddressAndPort, PendingState> states = new ConcurrentHashMap<>();

    public ConcurrentMap<InetAddressAndPort, PendingState> getStates()
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

    private PendingState getState(InetAddressAndPort endpoint)
    {
        PendingState state = states.get(endpoint);

        if (state == null)
        {
            PendingState newState = new PendingState();

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
        PendingState state = getState(endpoint);

        state.add(key);
    }

    private void remove(InetAddressAndPort endpoint, String key)
    {
        PendingState state = getState(endpoint);

        state.remove(key);
    }

    public Map<InetAddressAndPort, Long> getPendingCounts()
    {
        Map<InetAddressAndPort, Long> pendingCounts = new HashMap<>();

        for (Map.Entry<InetAddressAndPort, PendingState> entry : states.entrySet())
        {
            InetAddressAndPort endpoint = entry.getKey();
            PendingState state = entry.getValue();

            pendingCounts.put(endpoint, state.getPendingCount());
        }

        return pendingCounts;
    }

    public Map<InetAddressAndPort, Long> getFinishTimes()
    {
        Map<InetAddressAndPort, Long> finishTimes = new HashMap<>();

        for (Map.Entry<InetAddressAndPort, PendingState> entry : states.entrySet())
        {
            InetAddressAndPort endpoint = entry.getKey();
            PendingState state = entry.getValue();

            finishTimes.put(endpoint, state.getExpectedFinishTime());
        }

        return finishTimes;
    }
}
