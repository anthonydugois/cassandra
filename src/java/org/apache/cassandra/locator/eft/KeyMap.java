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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public final class KeyMap
{
    public final static KeyMap instance = new KeyMap();

    private final Map<String, Long> sizes = new HashMap<>();

    private boolean isLoaded = false;

    public void putInMemory(File file) throws IOException
    {
        try (BufferedReader reader = new BufferedReader(new FileReader(file)))
        {
            String line;

            while ((line = reader.readLine()) != null)
            {
                String[] cells = line.split(",");
                String key = cells[0];
                long size = Long.parseLong(cells[1]);

                sizes.put(key, size);
            }
        }
    }

    public boolean isLoaded()
    {
        return isLoaded;
    }

    public void setLoaded(boolean loaded)
    {
        isLoaded = loaded;
    }

    public Map<String, Long> getSizes()
    {
        return sizes;
    }

    public boolean containsKey(String key)
    {
        return sizes.containsKey(key);
    }
}
