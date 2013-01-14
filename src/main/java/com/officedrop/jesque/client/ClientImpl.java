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
import com.officedrop.redis.failover.jedis.JedisPool;

/**
 * Basic implementation of the Client interface.
 * 
 * @author Greg Haines
 */
public class ClientImpl extends AbstractClient
{
    private final JedisPool pool;

	/**
	 * Create a new ClientImpl, which creates it's own connection to Redis using values from the config.
	 * It will not verify the connection before use.
	 * 
	 * @param config used to create a connection to Redis
	 */
	public ClientImpl(final Config config, JedisPool pool)
	{
		super(config);
        this.pool = pool;
	}
	
	@Override
	protected void doEnqueue(final String queue, final String jobJson)
	{

		doEnqueue(this.pool, getNamespace(), queue, jobJson);
	}
	
	public void end()
	{
        this.pool.close();
	}
}
