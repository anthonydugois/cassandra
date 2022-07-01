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

package org.apache.cassandra.custom;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.concurrent.ScheduledExecutors;

public abstract class ScheduledRunnable implements Runnable
{
    protected ScheduledFuture<?> schedular = null;

    protected final long initialDelay;

    protected final long delay;

    protected final TimeUnit unit;

    protected ScheduledRunnable(long initialDelay, long delay)
    {
        this(initialDelay, delay, TimeUnit.MILLISECONDS);
    }

    protected ScheduledRunnable(long initialDelay, long delay, TimeUnit unit)
    {
        this.initialDelay = initialDelay;
        this.delay = delay;
        this.unit = unit;
    }

    public void start()
    {
        schedular = ScheduledExecutors.scheduledTasks.scheduleWithFixedDelay(this, initialDelay, delay, unit);
    }

    public void cancel()
    {
        if (schedular != null)
        {
            schedular.cancel(false);
        }
    }
}
