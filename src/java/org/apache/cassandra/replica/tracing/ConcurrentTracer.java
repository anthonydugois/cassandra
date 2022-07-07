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

package org.apache.cassandra.replica.tracing;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class ConcurrentTracer implements Runnable
{
    private final BufferedWriter writer;

    private final BlockingQueue<CharSequence> queue = new LinkedBlockingQueue<>();

    private volatile boolean stopped = false;

    public ConcurrentTracer(Writer writer)
    {
        this.writer = new BufferedWriter(writer);
    }

    public ConcurrentTracer(Writer writer, int sz)
    {
        this.writer = new BufferedWriter(writer, sz);
    }

    public void trace(CharSequence csq) throws InterruptedException
    {
        queue.put(csq);
    }

    public void start()
    {
        new Thread(this).start();
    }

    public void stop()
    {
        stopped = true;
    }

    @Override
    public void run()
    {
        while (!stopped)
        {
            try
            {
                CharSequence csq = queue.take();

                writer.append(csq);
            }
            catch (InterruptedException | IOException exception)
            {
                // ignored
            }
        }

        try
        {
            writer.close();
        }
        catch (IOException exception)
        {
            // ignored
        }
    }
}
