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
    public final static String DEFAULT_PATHNAME = "/csv/keymap.csv";

    public final static long DEFAULT_DELAY_MILLIS = 3 * 1000;

    public final static KeyMapLoader instance = new KeyMapLoader(DEFAULT_PATHNAME, DEFAULT_DELAY_MILLIS);

    public final static Logger logger = LoggerFactory.getLogger(KeyMapLoader.class);

    private ScheduledFuture<?> schedular;

    private final String pathname;

    private final long delay;

    public KeyMapLoader(String pathname, long delay)
    {
        this.pathname = pathname;
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
            File file = new File(pathname);

            if (file.exists())
            {
                cancel();

                try
                {
                    KeyMap.instance.putInMemory(file);

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
