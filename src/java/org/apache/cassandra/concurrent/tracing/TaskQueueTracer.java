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

package org.apache.cassandra.concurrent.tracing;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.eft.PendingState;
import org.apache.cassandra.locator.eft.PendingStates;

public class TaskQueueTracer
{
    public final static String DEFAULT_QUEUE_FILE = "csv/queue.csv";

    public final static String DEFAULT_STATE_FILE = "csv/state.csv";

    public final static long DEFAULT_DELAY_MILLIS = 100;

    public static final TaskQueueTracer instance = new TaskQueueTracer(new File(DEFAULT_QUEUE_FILE),
                                                                       new File(DEFAULT_STATE_FILE),
                                                                       DEFAULT_DELAY_MILLIS);

    private ScheduledFuture<?> schedular;

    private final long delay;

    private BufferedWriter queueFileWriter;

    private BufferedWriter stateFileWriter;

    public TaskQueueTracer(File queueFile, File stateFile, long delay)
    {
        this.delay = delay;

        try
        {
            queueFileWriter = new BufferedWriter(new FileWriter(queueFile, true));
            stateFileWriter = new BufferedWriter(new FileWriter(stateFile, true));
        }
        catch (Exception exception)
        {
            exception.printStackTrace();
        }
    }

    private void traceQueue(long epoch)
    {
        int count = Stage.READ.getPendingTaskCount();

        if (count > 0)
        {
            try
            {
                queueFileWriter.append(Long.toString(epoch)).append(",").append(Integer.toString(count)).append("\n");
            }
            catch (Exception exception)
            {
                exception.printStackTrace();
            }
        }
    }

    private void traceState(long epoch)
    {
        Map<InetAddressAndPort, PendingState> states = PendingStates.instance.getStates();

        if (states.size() > 0)
        {
            try
            {
                stateFileWriter.append(Long.toString(epoch));

                for (Map.Entry<InetAddressAndPort, PendingState> entry : states.entrySet())
                {
                    InetAddressAndPort endpoint = entry.getKey();
                    PendingState state = entry.getValue();

                    stateFileWriter.append(",").append(endpoint.toString()).append(",").append(Long.toString(state.getPendingCount()));
                }

                stateFileWriter.append("\n");
            }
            catch (Exception exception)
            {
                exception.printStackTrace();
            }
        }
    }

    public void trace()
    {
        long epoch = System.currentTimeMillis();

        traceQueue(epoch);

        traceState(epoch);
    }

    public void start()
    {
        schedular = ScheduledExecutors.scheduledTasks.scheduleWithFixedDelay(this::trace, 0, delay, TimeUnit.MILLISECONDS);
    }

    public void cancel()
    {
        schedular.cancel(false);
    }
}
