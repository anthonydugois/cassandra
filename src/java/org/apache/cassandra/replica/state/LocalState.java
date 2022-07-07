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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.locator.Replica;

public class LocalState
{
    public static final LocalState instance = new LocalState();

    private final ConcurrentMap<Replica, AtomicInteger> requestCounters = new ConcurrentHashMap<>();

    public AtomicInteger getRequestCounter(Replica replica)
    {
        AtomicInteger count = requestCounters.get(replica);

        if (count == null)
        {
            AtomicInteger newCount = new AtomicInteger(0);

            count = requestCounters.putIfAbsent(replica, newCount);

            if (count == null)
            {
                count = newCount;
            }
        }

        return count;
    }

    public void incrementPendingRequests(Iterable<Replica> replicas)
    {
        for (Replica replica : replicas)
        {
            AtomicInteger count = getRequestCounter(replica);

            count.incrementAndGet();
        }
    }

    public void decrementPendingRequests(Iterable<Replica> replicas)
    {
        for (Replica replica : replicas)
        {
            AtomicInteger count = getRequestCounter(replica);

            count.decrementAndGet();
        }
    }

    public Map<Replica, Integer> getPendingRequests()
    {
        Map<Replica, Integer> pendingRequests = new HashMap<>();

        for (Map.Entry<Replica, AtomicInteger> entry : requestCounters.entrySet())
        {
            pendingRequests.put(entry.getKey(), entry.getValue().get());
        }

        return pendingRequests;
    }

    public int getPendingReadCount()
    {
        return Stage.READ.getPendingTaskCount();
    }
}
