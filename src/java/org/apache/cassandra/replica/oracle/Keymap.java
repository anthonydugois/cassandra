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

package org.apache.cassandra.replica.oracle;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.replica.util.ScheduledRunnable;

public class Keymap
{
    public static final Keymap instance = new Keymap();

    private final Map<String, Integer> sizes = new HashMap<>();

    public Map<String, Integer> getSizes()
    {
        return sizes;
    }

    public int get(String key)
    {
        return sizes.get(key);
    }

    public boolean contains(String key)
    {
        return sizes.containsKey(key);
    }

    public void put(Path path) throws IOException
    {
        try (Stream<String> lines = Files.lines(path))
        {
            lines.forEach((line) -> {
                String[] cells = line.split(",", 2);
                sizes.put(cells[0], Integer.parseInt(cells[1]));
            });
        }
    }

    public static class Loader extends ScheduledRunnable
    {
        public final static Logger logger = LoggerFactory.getLogger(Loader.class);

        private final Set<Path> loadedPaths = new HashSet<>();

        private Supplier<Iterable<Path>> pathSupplier = null;

        public Loader(long delay)
        {
            super(delay);
        }

        public Loader(long initialDelay, long delay)
        {
            super(initialDelay, delay);
        }

        public Loader(long initialDelay, long delay, TimeUnit unit)
        {
            super(initialDelay, delay, unit);
        }

        public void setPathSupplier(Supplier<Iterable<Path>> pathSupplier)
        {
            this.pathSupplier = pathSupplier;
        }

        @Override
        public void run()
        {
            if (pathSupplier != null)
            {
                Iterable<Path> paths = pathSupplier.get();

                if (paths != null)
                {
                    for (Path path : paths)
                    {
                        if (!loadedPaths.contains(path))
                        {
                            try
                            {
                                Keymap.instance.put(path);
                            }
                            catch (IOException exception)
                            {
                                exception.printStackTrace();
                            }

                            loadedPaths.add(path);

                            logger.info("Keymap file " + path + " has been successfully put in memory");
                        }
                    }
                }
            }
        }
    }
}
