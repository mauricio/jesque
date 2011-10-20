package net.greghaines.jesque.worker;

import static net.greghaines.jesque.utils.ResqueConstants.FAILED;
import static net.greghaines.jesque.utils.ResqueConstants.STAT;

import java.io.IOException;
import java.util.Date;

import net.greghaines.jesque.Config;
import net.greghaines.jesque.Job;
import net.greghaines.jesque.JobFailure;
import net.greghaines.jesque.json.ObjectMapperFactory;
import net.greghaines.jesque.utils.JedisPool;
import net.greghaines.jesque.utils.JesqueUtils;
import redis.clients.jedis.Jedis;

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

		this.pool.withJedis( new JedisAction() {

			@Override
			public void execute(Jedis jedis) {
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
