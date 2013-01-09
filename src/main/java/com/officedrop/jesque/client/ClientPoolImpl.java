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
package com.officedrop.jesque.client;

import com.officedrop.jesque.Config;
import com.officedrop.jesque.utils.PoolUtils;
import com.officedrop.jesque.utils.PoolUtils.PoolWork;
import redis.clients.jedis.Jedis;
import redis.clients.util.Pool;

/**
 * A Client implementation that gets its connection to Redis from a connection pool.
 * 
 * @author Greg Haines
 */
public class ClientPoolImpl extends AbstractClient
{
	private final Pool<Jedis> jedisPool;

	/**
	 * Create a ClientPoolImpl.
	 * 
	 * @param config used to get the namespace for key creation 
	 * @param jedisPool the connection pool
	 */
	public ClientPoolImpl(final Config config, final Pool<Jedis> jedisPool)
	{
		super(config);
		if (jedisPool == null)
		{
			throw new IllegalArgumentException("jedisPool must not be null");
		}
		this.jedisPool = jedisPool;
	}

	@Override
	protected void doEnqueue(final String queue, final String jobJson)
	throws Exception
	{
		PoolUtils.doWorkInPool(this.jedisPool, new PoolWork<Jedis,Void>()
		{
			public Void doWork(final Jedis jedis)
			{
				doEnqueue(jedis, getNamespace(), queue, jobJson);
				return null;
			}
		});
	}

	/**
	 * Does nothing.
	 */
	public void end(){} // Do nothing
}
