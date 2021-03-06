/*
 * Copyright 2011 Greg Haines
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.officedrop.jesque;

import com.officedrop.jesque.client.Client;
import com.officedrop.jesque.client.ClientImpl;
import com.officedrop.jesque.worker.Worker;
import com.officedrop.redis.failover.jedis.JedisPool;
import com.officedrop.redis.failover.jedis.JedisPoolBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.List;

/**
 * Test helpers.
 * 
 * @author Greg Haines
 */
public final class TestUtils
{
	private static final Logger log = LoggerFactory.getLogger(TestUtils.class);

	/**
	 * Create a connection to Redis from the given Config.
	 * 
	 * @param config the location of the Redis server
	 * @return a new connection
	 */
	public static Jedis createJedis(final Config config)
	{
		if (config == null)
		{
			throw new IllegalArgumentException("config must not be null");
		}
		final Jedis jedis = new Jedis(config.getHost(), config.getPort(), config.getTimeout());
		if (config.getPassword() != null)
		{
			jedis.auth(config.getPassword());
		}
		jedis.select(config.getDatabase());
		return jedis;
	}
	
	public static void enqueueJobs(final String queue, final List<Job> jobs, final Config config)
	{

        JedisPool pool = new JedisPoolBuilder().withHost( config.getHost(), config.getPort() ).build();

		final Client client = new ClientImpl(config, pool);
		try
		{
			for (final Job job : jobs)
			{
				client.enqueue(queue, job);
			}
		}
		finally
		{
            pool.close();
			client.end();
		}
	}
	
	public static void stopWorker(final Worker worker, final Thread workerThread)
	{
		try { Thread.sleep(1000); } catch (Exception e){} // Give worker time to process
		worker.end(false);
		try
		{
			workerThread.join();
		}
		catch (Exception e)
		{
			log.warn("Exception while waiting for workerThread to join", e);
		}
	}

	private TestUtils(){} // Utility class
}
