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

package org.apache.cassandra.custom.state;

import java.io.IOException;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public class StatePayload
{
    private int pendingReadCount = 0;

    public int getPendingReadCount()
    {
        return pendingReadCount;
    }

    public void setPendingReadCount(int pendingReadCount)
    {
        this.pendingReadCount = pendingReadCount;
    }

    public boolean shouldSend()
    {
        return pendingReadCount > 0;
    }

    public static final IVersionedSerializer<StatePayload> serializer = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(StatePayload payload, DataOutputPlus out, int version) throws IOException
        {
            out.writeInt(payload.getPendingReadCount());
        }

        @Override
        public StatePayload deserialize(DataInputPlus in, int version) throws IOException
        {
            StatePayload payload = new StatePayload();

            payload.setPendingReadCount(in.readInt());

            return payload;
        }

        @Override
        public long serializedSize(StatePayload payload, int version)
        {
            return 4;
        }
    };
}
