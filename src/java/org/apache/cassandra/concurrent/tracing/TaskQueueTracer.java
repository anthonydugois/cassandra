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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.eft.PendingState;
import org.apache.cassandra.locator.eft.PendingStates;

public class TaskQueueTracer
{
    private static final Logger logger = LoggerFactory.getLogger(TaskQueueTracer.class);

    public final static String DEFAULT_QUEUE_FILE = "csv/queue.csv";

    public final static String DEFAULT_STATE_FILE = "csv/state.csv";

    public final static long DEFAULT_DELAY_MILLIS = 100;

    public static final long DEFAULT_FLUSH_PERIOD = 100;

    public static final TaskQueueTracer instance = new TaskQueueTracer(new File(DEFAULT_QUEUE_FILE),
                                                                       new File(DEFAULT_STATE_FILE),
                                                                       DEFAULT_DELAY_MILLIS,
                                                                       DEFAULT_FLUSH_PERIOD);

    private ScheduledFuture<?> schedular;

    private final long delay;

    private final long flushPeriod;

    private BufferedWriter queueFileWriter;

    private BufferedWriter stateFileWriter;

    private long period = 0;

    public TaskQueueTracer(File queueFile, File stateFile, long delay, long flushPeriod)
    {
        this.delay = delay;
        this.flushPeriod = flushPeriod;

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
        boolean trace = false;

        int count = Stage.READ.getPendingTaskCount();

        if (count > 0)
        {
            trace = true;
        }

        if (trace)
        {
            String line = epoch + "," + count + '\n';

            try
            {
                queueFileWriter.append(line);

                logger.info("Queue has been traced");
            }
            catch (Exception exception)
            {
                exception.printStackTrace();
            }
        }
    }

    private void traceState(long epoch)
    {
        boolean trace = false;

        Map<InetAddressAndPort, PendingState> states = PendingStates.instance.getStates();
        StringBuilder counts = new StringBuilder();

        for (Map.Entry<InetAddressAndPort, PendingState> entry : states.entrySet())
        {
            InetAddressAndPort endpoint = entry.getKey();
            PendingState state = entry.getValue();
            long count = state.getPendingCount();

            if (count > 0)
            {
                counts.append(',');
                counts.append(endpoint);
                counts.append(',');
                counts.append(state.getPendingCount());

                trace = true;
            }
        }

        if (trace)
        {
            String line = epoch + counts.toString() + '\n';

            try
            {
                stateFileWriter.append(line);

                logger.info("State has been traced");
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

        period++;

        if (period > flushPeriod)
        {
            period = 0;

            try
            {
                queueFileWriter.flush();
                stateFileWriter.flush();
            }
            catch (Exception exception)
            {
                exception.printStackTrace();
            }
        }
    }

    public void start()
    {
        logger.info("Starting queue/state tracer");

        schedular = ScheduledExecutors.scheduledTasks.scheduleWithFixedDelay(this::trace, delay, delay, TimeUnit.MILLISECONDS);
    }

    public void cancel()
    {
        schedular.cancel(false);
    }
}
