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

package org.apache.cassandra.custom.snitch;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.Writer;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.locator.AbstractEndpointSnitch;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;

public abstract class SelectionSnitch extends AbstractEndpointSnitch
{
    public final IEndpointSnitch subsnitch;

    private final File trace;

    private Writer writer;

    private final AtomicInteger flushPeriod = new AtomicInteger(0);

    protected SelectionSnitch(IEndpointSnitch subsnitch)
    {
        this.subsnitch = subsnitch;
        this.trace = new File("/csv/trace.csv");

        try
        {
            this.writer = new BufferedWriter(new FileWriter(trace, true));
        }
        catch (Exception exception)
        {
            exception.printStackTrace();
        }
    }

    public IEndpointSnitch getSubsnitch()
    {
        return subsnitch;
    }

    public void trace(String line)
    {
        trace(line, 1000);
    }

    public void trace(String line, int period)
    {
        try
        {
            writer.append(line);
        }
        catch (Exception exception)
        {
            exception.printStackTrace();
        }

        if (flushPeriod.incrementAndGet() > period)
        {
            flushPeriod.set(0);

            try
            {
                writer.flush();
            }
            catch (Exception exception)
            {
                exception.printStackTrace();
            }
        }
    }

    @Override
    public String getRack(InetAddressAndPort endpoint)
    {
        return subsnitch.getRack(endpoint);
    }

    @Override
    public String getDatacenter(InetAddressAndPort endpoint)
    {
        return subsnitch.getDatacenter(endpoint);
    }

    @Override
    public void gossiperStarting()
    {
        subsnitch.gossiperStarting();
    }
}
