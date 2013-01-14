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
import com.officedrop.jesque.Job;
import com.officedrop.jesque.json.ObjectMapperFactory;
import com.officedrop.jesque.utils.JesqueUtils;
import com.officedrop.redis.failover.jedis.JedisActions;
import com.officedrop.redis.failover.jedis.JedisFunction;
import com.officedrop.redis.failover.jedis.JedisPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.officedrop.jesque.utils.ResqueConstants.*;

/**
 * Common logic for Client implementations.
 * 
 * @author Greg Haines
 */
public abstract class AbstractClient implements Client
{
	private static final Logger log = LoggerFactory.getLogger(AbstractClient.class);
	
	private final String namespace;
	
	/**
	 * @param config used to get the namespace for key creation
	 */
	protected AbstractClient(final Config config)
	{
		if (config == null)
		{
			throw new IllegalArgumentException("config must not be null");
		}
		this.namespace = config.getNamespace();
	}
	
	protected String getNamespace()
	{
		return this.namespace;
	}

	public void enqueue(final String queue, final Job job)
	{
		if (queue == null || "".equals(queue))
		{
			throw new IllegalArgumentException("queue must not be null or empty: " + queue);
		}
		if (job == null)
		{
			throw new IllegalArgumentException("job must not be null");
		}
		if (!job.isValid())
		{
			throw new IllegalStateException("job is not valid: " + job);
		}
		try
		{
			doEnqueue(queue, ObjectMapperFactory.get().writeValueAsString(job));
		}
		catch (RuntimeException re)
		{
			throw re;
		}
		catch (Exception e)
		{
			throw new RuntimeException(e);
		}
	}

	/**
	 * Actually enqueue the serialized job.
	 * 
	 * @param queue the queue to add the Job to
	 * @param msg the serialized Job
	 * @throws Exception in case something goes wrong
	 */
	protected abstract void doEnqueue(String queue, String msg) throws Exception;

	/**
	 * Builds a namespaced Redis key with the given arguments.
	 * 
	 * @param parts the key parts to be joined
	 * @return an assembled String key
	 */
	protected String key(final String... parts)
	{
		return JesqueUtils.createKey(this.namespace, parts);
	}
	
	public static void doEnqueue(final JedisPool jedis, final String namespace,
			final String queue, final String jobJson)
	{

        jedis.withJedis(new JedisFunction() {
            @Override
            public void execute(final JedisActions jedisActions) throws Exception {
                jedisActions.sadd(QUEUES, queue);
                jedisActions.rpush( JesqueUtils.createKey(QUEUE, queue), jobJson );
            }
        });

	}
}
