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
import java.io.IOException;
import java.io.Writer;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.locator.AbstractEndpointSnitch;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;

public abstract class SelectionSnitch extends AbstractEndpointSnitch
{
    public static final String TRACE_PROPERTY = "trace";
    public static final String DEFAULT_TRACE_PROPERTY = "false";

    public static final String TRACE_FILE_PROPERTY = "trace_file";
    public static final String DEFAULT_TRACE_FILE_PROPERTY = "/csv/trace.csv";

    public final IEndpointSnitch subsnitch;

    private final Map<String, String> parameters;

    private final boolean trace;

    private final File traceFile;
    private Writer traceWriter;

    private final AtomicInteger flushPeriod = new AtomicInteger(0);

    protected SelectionSnitch(IEndpointSnitch subsnitch, Map<String, String> parameters)
    {
        this.subsnitch = subsnitch;
        this.parameters = parameters;

        this.trace = Boolean.parseBoolean(parameters.getOrDefault(TRACE_PROPERTY, DEFAULT_TRACE_PROPERTY));
        this.traceFile = new File(parameters.getOrDefault(TRACE_FILE_PROPERTY, DEFAULT_TRACE_FILE_PROPERTY));

        try
        {
            this.traceWriter = new BufferedWriter(new FileWriter(traceFile, true));
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

    public Map<String, String> getParameters()
    {
        return parameters;
    }

    public File getTraceFile()
    {
        return traceFile;
    }

    public Writer getTraceWriter()
    {
        return traceWriter;
    }

    public void trace(String line) throws IOException
    {
        trace(line, 1000);
    }

    public void trace(String line, int period) throws IOException
    {
        if (!trace)
        {
            return;
        }

        traceWriter.append(line);

        if (flushPeriod.incrementAndGet() > period)
        {
            flushPeriod.set(0);
            traceWriter.flush();
        }
    }

    public void flushTrace() throws IOException
    {
        traceWriter.flush();
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
