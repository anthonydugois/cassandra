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

package org.apache.cassandra.replica.broadcast;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.replica.util.ScheduledRunnable;
import org.apache.cassandra.utils.FBUtilities;

public class ScheduledBroadcast extends ScheduledRunnable
{
    private static final Logger logger = LoggerFactory.getLogger(ScheduledBroadcast.class);

    private BroadcastPayloadMessageFactory messageFactory;

    public ScheduledBroadcast(long delay)
    {
        super(delay);
    }

    public ScheduledBroadcast(long initialDelay, long delay)
    {
        super(initialDelay, delay);
    }

    public ScheduledBroadcast(long initialDelay, long delay, TimeUnit unit)
    {
        super(initialDelay, delay, unit);
    }

    public BroadcastPayloadMessageFactory getMessageFactory()
    {
        return messageFactory;
    }

    public void setMessageFactory(BroadcastPayloadMessageFactory messageFactory)
    {
        this.messageFactory = messageFactory;
    }

    public Set<InetAddressAndPort> getLiveEndpoints()
    {
        Set<InetAddressAndPort> endpoints = Gossiper.instance.getLiveMembers();

        endpoints.remove(FBUtilities.getBroadcastAddressAndPort());

        return endpoints;
    }

    private boolean shouldSend(Message<BroadcastPayload> message)
    {
        return message.payload.getPendingReadCount() > 0;
    }

    @Override
    public void run()
    {
        Message<BroadcastPayload> message = messageFactory.build();

        if (shouldSend(message))
        {
            for (InetAddressAndPort endpoint : getLiveEndpoints())
            {
                MessagingService.instance().send(message, endpoint);
            }
        }
    }
}
