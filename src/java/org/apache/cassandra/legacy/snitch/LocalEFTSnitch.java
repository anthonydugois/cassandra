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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaCollection;
import org.apache.cassandra.locator.SimpleSnitch;
import org.apache.cassandra.utils.FBUtilities;

public class LocalEFTSnitch extends SimpleSnitch
{
    private static final Logger logger = LoggerFactory.getLogger(LocalEFTSnitch.class);

    public final static String DEFAULT_TRACE_FILE = "csv/trace.csv";

    public final static long DEFAULT_PERIOD = 1000;

    private final File traceFile;

    private BufferedWriter writer;

    private long period = 0;

    public LocalEFTSnitch()
    {
        traceFile = new File(DEFAULT_TRACE_FILE);

        try
        {
            writer = new BufferedWriter(new FileWriter(traceFile, true));
        }
        catch (Exception exception)
        {
            exception.printStackTrace();
        }
    }

    @Override
    public <C extends ReplicaCollection<? extends C>> C sortedByProximity(InetAddressAndPort address, C unsortedAddress)
    {
        assert address.equals(FBUtilities.getBroadcastAddressAndPort());

        Map<InetAddressAndPort, Long> states = LocalPendingStates.instance.getPendingCounts(unsortedAddress);

        trace(states);

        logger.info("EFT states : " + states);

        return unsortedAddress.sorted((r1, r2) -> compareEndpoints(address, r1, r2, states));
    }

    private int compareEndpoints(InetAddressAndPort target, Replica r1, Replica r2, Map<InetAddressAndPort, Long> states)
    {
        Long t1 = states.getOrDefault(r1.endpoint(), 0L);
        Long t2 = states.getOrDefault(r2.endpoint(), 0L);

        return t1.compareTo(t2);
    }

    private void trace(Map<InetAddressAndPort, Long> states)
    {
        long epoch = System.currentTimeMillis();

        StringBuilder counts = new StringBuilder();

        for (Map.Entry<InetAddressAndPort, Long> entry : states.entrySet())
        {
            InetAddressAndPort endpoint = entry.getKey();
            long count = entry.getValue();

            counts.append(',').append(endpoint).append(',').append(count);
        }

        String line = epoch + counts.toString() + '\n';

        try
        {
            writer.append(line);
        }
        catch (Exception exception)
        {
            exception.printStackTrace();
        }

        period++;

        if (period > DEFAULT_PERIOD)
        {
            period = 0;

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
}
