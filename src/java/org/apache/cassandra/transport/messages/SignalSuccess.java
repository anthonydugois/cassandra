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
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.ProtocolVersion;

public class SignalSuccess extends Message.Response
{
    public static final Message.Codec<SignalSuccess> codec = new Message.Codec<>()
    {
        @Override
        public SignalSuccess decode(ByteBuf body, ProtocolVersion version)
        {
            return new SignalSuccess();
        }

        @Override
        public void encode(SignalSuccess signalSuccess, ByteBuf dest, ProtocolVersion version)
        {

        }

        @Override
        public int encodedSize(SignalSuccess signalSuccess, ProtocolVersion version)
        {
            return 0;
        }
    };

    public SignalSuccess()
    {
        super(Message.Type.SIGNAL_SUCCESS);
    }

    @Override
    public String toString()
    {
        return "SIGNAL_SUCCESS";
    }
}
