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

import java.io.File;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;

public class KeyMapLoader
{
    public final static String DEFAULT_CSV_DIR = "/csv";

    public final static String DEFAULT_CSV_PREFIX = "keymap";

    public final static long DEFAULT_DELAY_MILLIS = 60000;

    public final static KeyMapLoader instance = new KeyMapLoader(new File(DEFAULT_CSV_DIR), DEFAULT_CSV_PREFIX, DEFAULT_DELAY_MILLIS);

    public final static Logger logger = LoggerFactory.getLogger(KeyMapLoader.class);

    private ScheduledFuture<?> schedular;

    private final File dir;

    private final String prefix;

    private final long delay;

    public KeyMapLoader(File dir, String prefix, long delay)
    {
        this.dir = dir;
        this.prefix = prefix;
        this.delay = delay;
    }

    private void tryLoading()
    {
        if (KeyMap.instance.isLoaded())
        {
            cancel();
        }
        else
        {
            if (dir.exists())
            {
                cancel();

                File[] files = dir.listFiles((file, name) -> name.toLowerCase().startsWith(prefix));

                try
                {
                    for (int i = 0; i < files.length; ++i)
                    {
                        KeyMap.instance.putInMemory(files[i]);
                    }

                    KeyMap.instance.setLoaded(true);

                    logger.info("Keymap has been successfully put in memory");
                }
                catch (Exception exception)
                {
                    exception.printStackTrace();
                }
            }
        }
    }

    public void start()
    {
        schedular = ScheduledExecutors.scheduledTasks.scheduleWithFixedDelay(this::tryLoading, delay, delay, TimeUnit.MILLISECONDS);
    }

    public void cancel()
    {
        schedular.cancel(false);
    }
}
