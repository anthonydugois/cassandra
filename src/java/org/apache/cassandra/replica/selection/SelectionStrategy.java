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

package org.apache.cassandra.replica.selection;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import org.apache.cassandra.locator.AbstractEndpointSnitch;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.replica.tracing.ConcurrentTracer;

public abstract class SelectionStrategy extends AbstractEndpointSnitch
{
    public static final String TRACING_PROPERTY = "tracing";
    public static final String DEFAULT_TRACING_PROPERTY = "false";

    protected final boolean tracing;

    public static final String TRACE_FILE_PROPERTY = "trace_file";
    public static final String DEFAULT_TRACE_FILE_PROPERTY = "/csv/trace.csv";

    protected final File traceFile;

    protected final IEndpointSnitch snitch;

    protected final Map<String, String> parameters;

    protected ConcurrentTracer tracer = null;

    protected SelectionStrategy(IEndpointSnitch snitch, Map<String, String> parameters)
    {
        this.snitch = snitch;
        this.parameters = parameters;

        this.tracing = Boolean.parseBoolean(parameters.getOrDefault(TRACING_PROPERTY, DEFAULT_TRACING_PROPERTY));
        this.traceFile = new File(parameters.getOrDefault(TRACE_FILE_PROPERTY, DEFAULT_TRACE_FILE_PROPERTY));

        try (FileWriter writer = new FileWriter(this.traceFile, true))
        {
            this.tracer = new ConcurrentTracer(writer);
        }
        catch (IOException exception)
        {
            exception.printStackTrace();
        }

        if (this.tracing && this.tracer != null)
        {
            this.tracer.start();
        }
    }

    public IEndpointSnitch getSnitch()
    {
        return snitch;
    }

    public Map<String, String> getParameters()
    {
        return parameters;
    }

    public boolean shouldTrace()
    {
        return tracing && tracer != null;
    }

    public void trace(CharSequence csq)
    {
        try
        {
            tracer.trace(csq);
        }
        catch (InterruptedException exception)
        {
            // ignored
        }
    }

    @Override
    public String getRack(InetAddressAndPort endpoint)
    {
        return snitch.getRack(endpoint);
    }

    @Override
    public String getDatacenter(InetAddressAndPort endpoint)
    {
        return snitch.getDatacenter(endpoint);
    }

    @Override
    public void gossiperStarting()
    {
        snitch.gossiperStarting();
    }
}
