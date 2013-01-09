package com.officedrop.jesque.worker;

import com.officedrop.jesque.Config;
import com.officedrop.jesque.Job;
import com.officedrop.jesque.JobFailure;
import com.officedrop.jesque.json.ObjectMapperFactory;
import com.officedrop.jesque.utils.JesqueUtils;
import com.officedrop.redis.failover.jedis.JedisActions;
import com.officedrop.redis.failover.jedis.JedisFunction;
import com.officedrop.redis.failover.jedis.JedisPool;

import java.io.IOException;
import java.util.Date;

import static com.officedrop.jesque.utils.ResqueConstants.*;

public class RedisFailureBackend implements FailureBackend {

	private Config config;	
	private String namespace;
	private JedisPool pool;
	
	public RedisFailureBackend(Config config, JedisPool jedisPool) {
		this.config = config;
		this.namespace = this.config.getNamespace();
		this.pool = jedisPool;
	}

	/**
	 * Builds a namespaced Redis key with the given arguments.
	 * 
	 * @param parts
	 *            the key parts to be joined
	 * @return an assembled String key
	 */
	private String key(final String... parts) {
		return JesqueUtils.createKey(this.namespace, parts);
	}
	
	@Override
	public void onFailure(final Worker worker, final Job job, final String queue, final Throwable exception) {

		this.pool.withJedis( new JedisFunction() {

			@Override
			public void execute(JedisActions jedis) {
				jedis.incr(key(STAT, FAILED));
				jedis.incr(key(STAT, FAILED, worker.getName() ));
				jedis.rpush(key(FAILED), failMsg(worker, exception, queue, job));
			}

		});		
		
	}
	
	/**
	 * Create and serialize a JobFailure.
	 * 
	 * @param ex
	 *            the Exception that occured
	 * @param queue
	 *            the queue the job came from
	 * @param job
	 *            the Job that failed
	 * @return the JSON representation of a new JobFailure
	 * @throws IOException
	 *             if there was an error serializing the JobFailure
	 */
	private String failMsg(Worker worker, Throwable ex, final String queue, final Job job) {
		final JobFailure f = new JobFailure();
		f.setFailedAt(new Date());
		f.setWorker( worker.getName() );
		f.setQueue(queue);
		f.setPayload(job);
		f.setException(ex);
		try {
			return ObjectMapperFactory.get().writeValueAsString(f);
		} catch (Exception e) {
			throw new RuntimeException(
					"Failed while generating error message for queue " + queue,
					e);
		}
	}

}
