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

package org.apache.cassandra.transport.messages;

import io.netty.buffer.ByteBuf;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.ProtocolVersion;

public class SignalMessage extends Message.Request
{
    public static final Message.Codec<SignalMessage> codec = new Message.Codec<>()
    {
        @Override
        public SignalMessage decode(ByteBuf body, ProtocolVersion version)
        {
            return new SignalMessage();
        }

        @Override
        public void encode(SignalMessage msg, ByteBuf dest, ProtocolVersion version)
        {
        }

        @Override
        public int encodedSize(SignalMessage msg, ProtocolVersion version)
        {
            return 0;
        }
    };

    public SignalMessage()
    {
        super(Message.Type.SIGNAL);
    }

    @Override
    protected Response execute(QueryState queryState, long queryStartNanoTime, boolean traceRequest)
    {
        try
        {
            // KeyMap.instance.putInMemory("src/resources/org/apache/cassandra/keymap.csv");
        }
        catch (Exception exception)
        {
            exception.printStackTrace();
        }

        return new SignalSuccess();
    }

    @Override
    public String toString()
    {
        return "SIGNAL";
    }
}
