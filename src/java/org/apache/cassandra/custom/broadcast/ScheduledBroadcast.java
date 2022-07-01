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

package org.apache.cassandra.custom.broadcast;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.custom.ScheduledRunnable;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.FBUtilities;

public class ScheduledBroadcast extends ScheduledRunnable
{
    private static final Logger logger = LoggerFactory.getLogger(ScheduledBroadcast.class);

    private MessageFactory<?> messageFactory;

    public ScheduledBroadcast(long initialDelay, long delay)
    {
        super(initialDelay, delay);
    }

    public ScheduledBroadcast(long initialDelay, long delay, TimeUnit unit)
    {
        super(initialDelay, delay, unit);
    }

    public void setMessageFactory(MessageFactory<?> messageFactory)
    {
        this.messageFactory = messageFactory;
    }

    @Override
    public void run()
    {
        Message<?> message = messageFactory.build();

        if (message != null)
        {
            InetAddressAndPort local = FBUtilities.getBroadcastAddressAndPort();
            Set<InetAddressAndPort> endpoints = Gossiper.instance.getLiveMembers();

            for (InetAddressAndPort endpoint : endpoints)
            {
                if (endpoint != local)
                {
                    MessagingService.instance().send(message, endpoint);

                    // logger.info("Sending " + message + " to " + endpoint);
                }
            }
        }
    }
}
