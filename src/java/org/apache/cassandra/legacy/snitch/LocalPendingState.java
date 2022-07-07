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

import java.util.concurrent.atomic.AtomicLong;

import org.apache.cassandra.legacy.keymap.KeyMap;

public class LocalPendingState
{
    private final AtomicLong pendingCount = new AtomicLong(0);

    private final AtomicLong expectedFinishTimeNanos = new AtomicLong(0);

    public long getPendingCount()
    {
        return pendingCount.get();
    }

    public long getExpectedFinishTime()
    {
        return expectedFinishTimeNanos.get();
    }

    private void increasePendingCount()
    {
        pendingCount.incrementAndGet();
    }

    private void decreasePendingCount()
    {
        pendingCount.decrementAndGet();
    }

    private void updateExpectedFinishTime(String key)
    {
        long size = KeyMap.instance.getSizes().get(key);
        long processingTimeNanos = getProcessingTime(size);

        long currentTimeNanos, finishTimeNanos, newFinishTimeNanos;

        do
        {
            currentTimeNanos = System.nanoTime();
            finishTimeNanos = expectedFinishTimeNanos.get();
            newFinishTimeNanos = Math.max(currentTimeNanos, finishTimeNanos) + processingTimeNanos;
        } while (!expectedFinishTimeNanos.compareAndSet(finishTimeNanos, newFinishTimeNanos));
    }

    public void add(String key)
    {
        increasePendingCount();

        updateExpectedFinishTime(key);
    }

    public void remove(String key)
    {
        decreasePendingCount();
    }

    public static long getProcessingTime(long size)
    {
        return size;
    }
}
