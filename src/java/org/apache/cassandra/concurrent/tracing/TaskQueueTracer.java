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
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.concurrent.Stage;

public class TaskQueueTracer
{
    public final static String DEFAULT_PATHNAME = "/csv/queue.csv";

    public final static long DEFAULT_DELAY_MILLIS = 100;

    public static final TaskQueueTracer instance = new TaskQueueTracer(new File(DEFAULT_PATHNAME), DEFAULT_DELAY_MILLIS);

    private ScheduledFuture<?> schedular;

    private final File file;

    private final long delay;

    private final List<Long> epochs = new ArrayList<>();

    private final List<Integer> taskCount = new ArrayList<>();

    private BufferedWriter writer;

    public TaskQueueTracer(File file, long delay)
    {
        this.file = file;
        this.delay = delay;

        try
        {
            writer = new BufferedWriter(new FileWriter(file, true));
        }
        catch (Exception exception)
        {
            exception.printStackTrace();
        }
    }

    public void trace()
    {
        Long epoch = System.currentTimeMillis();
        Integer count = Stage.READ.getPendingTaskCount();

        epochs.add(epoch);
        taskCount.add(count);

        try
        {
            writer.append(epoch.toString()).append(",").append(count.toString()).append("\n");
        }
        catch (Exception exception)
        {
            exception.printStackTrace();
        }
    }

    public int size()
    {
        return taskCount.size();
    }

    public void save() throws IOException
    {
        for (int i = 0; i < size(); ++i)
        {
            Long epoch = epochs.get(i);
            Integer count = taskCount.get(i);

            writer.append(epoch.toString()).append(",").append(count.toString()).append("\n");
        }
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
