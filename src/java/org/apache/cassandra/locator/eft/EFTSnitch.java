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

package org.apache.cassandra.locator.eft;

import java.util.Map;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaCollection;
import org.apache.cassandra.locator.SimpleSnitch;
import org.apache.cassandra.utils.FBUtilities;

public class EFTSnitch extends SimpleSnitch
{
    @Override
    public <C extends ReplicaCollection<? extends C>> C sortedByProximity(InetAddressAndPort address, C unsortedAddress)
    {
        assert address.equals(FBUtilities.getBroadcastAddressAndPort());

        Map<InetAddressAndPort, Long> finishTimes = PendingStates.instance.snapshot();

        return unsortedAddress.sorted((r1, r2) -> compareEndpoints(address, r1, r2, finishTimes));
    }

    private int compareEndpoints(InetAddressAndPort target, Replica r1, Replica r2, Map<InetAddressAndPort, Long> finishTimes)
    {
        Long t1 = finishTimes.getOrDefault(r1.endpoint(), 0L);
        Long t2 = finishTimes.getOrDefault(r2.endpoint(), 0L);

        return t1.compareTo(t2);
    }
}
