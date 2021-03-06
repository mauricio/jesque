package com.officedrop.jesque.worker;

import com.officedrop.jesque.Config;
import com.officedrop.jesque.JobFailure;
import com.officedrop.jesque.json.ObjectMapperFactory;
import com.officedrop.jesque.utils.JesqueUtils;
import com.officedrop.redis.failover.jedis.JedisActions;
import com.officedrop.redis.failover.jedis.JedisFunction;
import com.officedrop.redis.failover.jedis.JedisPool;
import com.officedrop.redis.failover.jedis.JedisResultFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

public class RetryCheckTimer extends TimerTask {

	private static final Logger log = LoggerFactory
			.getLogger(RetryCheckTimer.class);

	private Timer timer;
	private JedisPool pool;
	private Config config;

	public RetryCheckTimer(JedisPool pool, Config config, int interval) {
		this.pool = pool;
		this.config = config;
		this.timer = new Timer("retry-check-timer");

		long time = TimeUnit.MINUTES.toMillis(interval);

		log.warn(String.format("Configuring timer to run at every %d millis",
				time));

		this.timer.scheduleAtFixedRate(this, time, time);
	}

	public void stop() {
		this.timer.cancel();
	}

	@Override
	public void run() {

		log.warn("Starting retry process run");

		int start = 0;

		Map<String, JobFailure> failures = null;

		int retries = 0;
		int ignores = 0;

		final List<String> itemsToDelete = new LinkedList<String>();

		while (!(failures = load(start)).isEmpty()) {
			start += 50;

			for (Entry<String, JobFailure> entry : failures.entrySet()) {

				final JobFailure failure = entry.getValue();
				final String value = entry.getKey();

				Map<String, Object> jobData = (Map<String, Object>) failure
						.getPayload().getArgs()[0];
				Map<String, Object> retryData = (Map<String, Object>) jobData
						.get(RetryFailureBackend.RETRY_DATA);

				long nextRetry = (Long) retryData
						.get(RetryFailureBackend.NEXT_RETRY_AT);

				if (System.currentTimeMillis() >= nextRetry) {
					retries++;
					itemsToDelete.add(value);

					log.warn(String.format("Sending job for retry %s", value));

					this.retry(failure);

				} else {
					log.warn(String.format(
							"Job will wait %d minutes to be retried - %s",
							TimeUnit.MILLISECONDS.toMinutes(nextRetry
									- System.currentTimeMillis()), value));
					ignores++;
				}

			}

		}

		this.pool.withJedis(new JedisFunction() {

			@Override
			public void execute(JedisActions jedis) throws Exception {

				for (final String item : itemsToDelete) {

					jedis.lrem(key(RetryFailureBackend.PENDING_RETRY_QUEUE), 0,
							item);
				}
			}

		});

		log.warn(String.format("Processed %d items (%d retried - %d ignored)",
				retries + ignores, retries, ignores));

	}

	public Map<String, JobFailure> load(final int start) {

		List<String> values = this.pool
				.withJedis(new JedisResultFunction<List<String>>() {

					@Override
					public List<String> execute(JedisActions jedis) throws Exception {

						return jedis.lrange(
								key(RetryFailureBackend.PENDING_RETRY_QUEUE),
								start, 50);
					}

				});

		Map<String, JobFailure> failures = new HashMap<String, JobFailure>();

		for (String value : values) {

			try {

				failures.put(
						value,
						ObjectMapperFactory.get().readValue(value,
								JobFailure.class));

			} catch (Exception e) {
				throw new RuntimeException(String.format(
						"Error while trying to parse JSON - %s", value), e);
			}

		}

		return failures;
	}

	protected void retry(final JobFailure failure) {
		this.pool.withJedis(new JedisFunction() {

			@Override
			public void execute(JedisActions jedis) throws Exception {

				String queueName = failure.getQueue();

				if (!queueName.startsWith("retry_")) {
					queueName = "retry_" + queueName;
				}

				jedis.sadd(key("queues"), queueName);
				jedis.rpush(key("queue:" + queueName), ObjectMapperFactory
						.get().writeValueAsString(failure.getPayload()));

			}
		});
	}

	protected String key(final String... parts) {
		return JesqueUtils.createKey(this.config.getNamespace(), parts);
	}

}
