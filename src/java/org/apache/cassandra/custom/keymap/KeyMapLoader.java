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

package org.apache.cassandra.custom.keymap;

import java.io.File;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.custom.ScheduledRunnable;

public class KeyMapLoader extends ScheduledRunnable
{
    public final static Logger logger = LoggerFactory.getLogger(KeyMapLoader.class);

    public final static File DEFAULT_CSV_DIR = new File("/csv");

    public final static String DEFAULT_CSV_PREFIX = "keymap";

    public final static long DEFAULT_INITIAL_DELAY = 0;

    public final static long DEFAULT_DELAY = 60000;

    public final static KeyMapLoader instance = new KeyMapLoader(DEFAULT_INITIAL_DELAY,
                                                                 DEFAULT_DELAY,
                                                                 DEFAULT_CSV_DIR,
                                                                 DEFAULT_CSV_PREFIX);

    private final File dir;

    private final String prefix;

    private Set<String> loadedFiles = new HashSet<>();

    public KeyMapLoader(long initialDelay, long delay, File dir, String prefix)
    {
        super(initialDelay, delay, TimeUnit.MILLISECONDS);

        this.dir = dir;
        this.prefix = prefix;
    }

    @Override
    public void run()
    {
        if (dir.exists())
        {
            File[] files = dir.listFiles((file, name) -> name.toLowerCase().startsWith(prefix));

            if (files != null)
            {
                try
                {
                    for (int i = 0; i < files.length; ++i)
                    {
                        File file = files[i];
                        String path = file.getAbsolutePath();

                        if (!loadedFiles.contains(path))
                        {
                            KeyMap.instance.putInMemory(file);

                            loadedFiles.add(path);

                            logger.info("Keymap file " + path + " has been successfully put in memory");
                        }
                    }
                }
                catch (Exception exception)
                {
                    exception.printStackTrace();
                }
            }
            else
            {
                logger.info("No keymap file yet");
            }
        }
        else
        {
            logger.error("No keymap directory");
        }
    }
}
