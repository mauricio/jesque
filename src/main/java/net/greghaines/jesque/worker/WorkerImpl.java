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
package net.greghaines.jesque.worker;

import static net.greghaines.jesque.utils.ResqueConstants.COLON;
import static net.greghaines.jesque.utils.ResqueConstants.DATE_FORMAT;
import static net.greghaines.jesque.utils.ResqueConstants.FAILED;
import static net.greghaines.jesque.utils.ResqueConstants.JAVA_DYNAMIC_QUEUES;
import static net.greghaines.jesque.utils.ResqueConstants.PROCESSED;
import static net.greghaines.jesque.utils.ResqueConstants.QUEUE;
import static net.greghaines.jesque.utils.ResqueConstants.QUEUES;
import static net.greghaines.jesque.utils.ResqueConstants.STARTED;
import static net.greghaines.jesque.utils.ResqueConstants.STAT;
import static net.greghaines.jesque.utils.ResqueConstants.WORKER;
import static net.greghaines.jesque.utils.ResqueConstants.WORKERS;
import static net.greghaines.jesque.worker.WorkerEvent.JOB_EXECUTE;
import static net.greghaines.jesque.worker.WorkerEvent.JOB_FAILURE;
import static net.greghaines.jesque.worker.WorkerEvent.JOB_PROCESS;
import static net.greghaines.jesque.worker.WorkerEvent.JOB_SUCCESS;
import static net.greghaines.jesque.worker.WorkerEvent.WORKER_ERROR;
import static net.greghaines.jesque.worker.WorkerEvent.WORKER_POLL;
import static net.greghaines.jesque.worker.WorkerEvent.WORKER_START;
import static net.greghaines.jesque.worker.WorkerEvent.WORKER_STOP;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import net.greghaines.jesque.Config;
import net.greghaines.jesque.Job;
import net.greghaines.jesque.WorkerStatus;
import net.greghaines.jesque.json.ObjectMapperFactory;
import net.greghaines.jesque.utils.DaemonThreadFactory;
import net.greghaines.jesque.utils.JedisPool;
import net.greghaines.jesque.utils.JesqueUtils;
import net.greghaines.jesque.utils.ReflectionUtils;
import net.greghaines.jesque.utils.VersionUtils;

import org.codehaus.jackson.JsonParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;



/**
 * Basic implementation of the Worker interface. Obeys the contract of a Resque
 * worker in Redis.
 * 
 * @author Greg Haines
 */
public class WorkerImpl extends BaseWorker implements Worker {

	private static final Logger log = LoggerFactory.getLogger(WorkerImpl.class);
	private static final AtomicLong workerCounter = new AtomicLong(0);
	private static final long SLEEP_INCREMENT = 500;
	private static final int MAX_MISSES = 30;
	private static final long MAX_JOB_WAIT_TIME = 30;

	private final String namespace;
	private final String jobPackage;
	private final BlockingDeque<String> queueNames;
	private final String name;
	private final AtomicReference<WorkerState> state = new AtomicReference<WorkerState>(
			WorkerState.NEW);
	private final long workerId = workerCounter.getAndIncrement();
	private final String threadNameBase = "Worker-" + this.workerId
			+ " Jesque-" + VersionUtils.getVersion() + ": ";
	private int misses = 0;
	private JedisPool jedisPool;
	private ExecutorService threadPool = Executors
			.newCachedThreadPool(DaemonThreadFactory.getInstance());
	private Date currentJobStartTime;
	private FailureBackend failureBackend;

	private static final DateFormat LOG_DATE_FORMAT = new SimpleDateFormat(
			"MM/dd/yyyy-HH:mm:ss");
	private static final DateFormat RESQUE_DATE_FORMAT = new SimpleDateFormat(
			DATE_FORMAT);

	/**
	 * Creates a new WorkerImpl, which creates it's own connection to Redis
	 * using values from the config. The worker will only listen to the supplied
	 * queues and only execute jobs that are in the supplied job types.
	 * 
	 * @param config
	 *            used to create a connection to Redis and the package prefix
	 *            for incoming jobs
	 * @param queues
	 *            the list of queues to poll
	 * @param jobTypes
	 *            the list of job types to execute
	 * @throws IllegalArgumentException
	 *             if the config is null, if the queues is null, or if the
	 *             jobTypes is null or empty
	 */
	public WorkerImpl(Config config, Collection<String> queues,
			JedisPool jedisPool) {
		this(config, queues, jedisPool, new RedisFailureBackend(config,
				jedisPool));
	}	
	
	public WorkerImpl(Config config, Collection<String> queues,
			JedisPool jedisPool, FailureBackend failureBackend) {

		if (config == null) {
			throw new IllegalArgumentException("config must not be null");
		}

		WorkerUtils.checkQueues(queues);

		this.namespace = config.getNamespace();
		this.jobPackage = config.getJobPackage();
		this.failureBackend = failureBackend;

		this.queueNames = new LinkedBlockingDeque<String>(queues);

		this.name = createName();
		this.jedisPool = jedisPool;
	}

	@Override
	public WorkerState getState() {
		return this.state.get();
	}

	public String getNamespace() {
		return namespace;
	}

	public JedisPool getJedisPool() {
		return jedisPool;
	}

	/**
	 * Starts this worker. Registers the worker in Redis and begins polling the
	 * queues for jobs. Stop this worker by calling end() on any thread.
	 */
	public void run() {
		if (this.state.compareAndSet(WorkerState.NEW, WorkerState.RUNNING)) {
			try {
				this.register();
				this.poll();
			} catch (RuntimeException t) {
				this.state.set(WorkerState.FAILED);
				throw t;
			} finally {
				this.unregister();
				this.threadPool.shutdown();
			}
		} else {
			if (WorkerState.RUNNING.equals(this.state.get())) {
				throw new IllegalStateException(
						"This WorkerImpl is already running");
			} else {
				throw new IllegalStateException("This WorkerImpl is shutdown");
			}
		}
	}

	protected void register() {

		this.unregisterWorker();

		this.jedisPool.withJedis(new JedisAction() {

			@Override
			public void execute(Jedis jedis) {
				jedis.sadd(key(WORKERS), getName());
				jedis.set(key(WORKER, getName(), STARTED),
						RESQUE_DATE_FORMAT.format(new Date()));
			}

		});

		this.fireEvent(WORKER_START, this, null, null, null, null, null);
	}

	protected void unregister() {
		this.fireEvent(WORKER_STOP, this, null, null, null, null, null);

		this.unregisterWorker();
	}

	protected void unregisterWorker() {

		this.jedisPool.withJedis(new JedisAction() {

			@Override
			public void execute(Jedis jedis) throws Exception {

				Set<String> keys = jedis.smembers(key(WORKERS));

				for (String key : keys) {

					if (key.contains(InetAddress.getLocalHost().getHostName())) {
						jedis.srem(key(WORKERS), key);
						jedis.del(key(WORKER, key), key(WORKER, key, STARTED),
								key(STAT, FAILED, key),
								key(STAT, PROCESSED, key));
					}

				}

			}

		});

	}

	/**
	 * Shutdown this Worker.<br/>
	 * <b>The worker cannot be started again; create a new worker in this
	 * case.</b>
	 * 
	 * @param now
	 *            if true, an effort will be made to stop any job in progress
	 */
	public void end(final boolean now) {

		this.state.set(WorkerState.SHUTDOWN);

		if (now) {
			this.threadPool.shutdownNow();
		}
	}

	public void togglePause(final boolean paused) {

		if (paused) {
			this.state.set(WorkerState.PAUSED);
		} else {
			this.state.set(WorkerState.RUNNING);
		}

	}

	/**
	 * Polls the queues for jobs and executes them.
	 */
	private void poll() {
		while (this.isRunning()) {

			String curQueue = null;

			try {
				renameThread("Waiting for " + this.queueNames + " since "
						+ LOG_DATE_FORMAT.format(new Date()));

				Iterator<String> queues = new LinkedList<String>(
						this.queueNames).iterator();

				boolean executedJob = false;
				
				while (queues.hasNext()) {
					curQueue = queues.next();
					
					if ( executedJob = this.pollFromQueue(curQueue) ) {
						break;
					}
					
				}

				if ( !executedJob ) {
					this.sleep();	
				}				

			} catch (Exception e) {
				log.error("Job failed", e);
				this.fireEvent(WORKER_ERROR, this, curQueue, null, null, null,
						e);
			}
		}
	}

	protected boolean pollFromQueue(String queue) throws InterruptedException,
			JsonParseException, IOException {

		String payload = null;

		while (this.isRunning() && (payload = this.pop(queue)) != null) {
			this.misses = 0;
			
			final Job job = ObjectMapperFactory.get().readValue(payload,
					Job.class);

			if (this.shouldProcess(queue, payload, job)) {
				process(job, queue);
				return true;
			} else {
				break;
			}

		}

		if (this.misses < MAX_MISSES) {
			this.misses++;
		}

		return false;
	}

	protected boolean shouldProcess(String queue, String payload, Job job) {
		return true;
	}

	public boolean isRunning() {
		return this.state.get() != WorkerState.FAILED
				&& this.state.get() != WorkerState.SHUTDOWN;
	}

	private String pop(final String queue) {

		this.checkPaused();
		this.fireEvent(WORKER_POLL, this, queue, null, null, null, null);

		return this.jedisPool.withJedis(new JedisResultAction<String>() {

			@Override
			public String execute(Jedis jedis) {
				return jedis.lpop(key(QUEUE, queue));
			}
			
		});

	}

	/**
	 * Checks to see if worker is paused. If so, wait until unpaused.
	 */
	private void checkPaused() {

		while (this.isPaused()) {
			this.sleep();
		}

	}

	public boolean isPaused() {
		return this.state.get() == WorkerState.PAUSED;
	}

	public boolean isFailed() {
		return this.state.get() == WorkerState.FAILED;
	}

	private void sleep() {
		try {

			long time = SLEEP_INCREMENT * (this.misses + 1);

			renameThread(String.format("Paused for %s seconds since %s %s",
					time / 1000, 
					LOG_DATE_FORMAT.format(new Date()),
					this.queueNames
					));

			Thread.sleep(time);
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}

	/**
	 * Materializes and executes the given job.
	 * 
	 * @param job
	 *            the Job to process
	 * @param curQueue
	 *            the queue the payload came from
	 */
	private void process(final Job job, final String curQueue) {
		this.fireEvent(JOB_PROCESS, this, curQueue, job, null, null, null);
		renameThread("Processing " + curQueue + " since "
				+ LOG_DATE_FORMAT.format(new Date()));
		try {
			final String fullClassName = (this.jobPackage.length() == 0) ? job
					.getClassName() : this.jobPackage + "."
					+ job.getClassName();

			final Class<?> clazz = ReflectionUtils.forName(fullClassName);

			if (!Runnable.class.isAssignableFrom(clazz)
					&& !Callable.class.isAssignableFrom(clazz)) {
				throw new ClassCastException(
						"jobs must be a Runnable or a Callable: "
								+ clazz.getName() + " - " + job);
			}

			execute(job, curQueue,
					ReflectionUtils.createObject(clazz, job.getArgs()));
		} catch (Exception e) {
			log.error("Job failed", e);
			failure(e, job, curQueue);
		}
	}

	/**
	 * Executes the given job.
	 * 
	 * @param job
	 *            the job to execute
	 * @param curQueue
	 *            the queue the job came from
	 * @param instance
	 *            the materialized job
	 * @throws Exception
	 *             if the instance is a callable and throws an exception
	 */
	private void execute(final Job job, final String curQueue,
			final Object instance) throws Exception {

		this.jedisPool.withJedis(new JedisAction() {

			@Override
			public void execute(Jedis jedis) {
				jedis.set(key(WORKER, getName()), statusMsg(curQueue, job));
			}

		});

		FutureTask task = null;
		
		try {
			Object result = null;
			this.currentJobStartTime = new Date();
			this.fireEvent(JOB_EXECUTE, this, curQueue, job, instance, null,
					null);

			if (instance instanceof Callable) {

				Callable<?> callable = (Callable<?>) instance;
				task = new FutureTask(callable);

				this.threadPool.submit(task);
				
				result = task.get( MAX_JOB_WAIT_TIME , TimeUnit.MINUTES);

			} else if (instance instanceof Runnable) {

				Runnable runnable = (Runnable) instance;
				task = new FutureTask<String>(runnable,
						"finished");

				this.threadPool.submit(task);

				result = task.get( MAX_JOB_WAIT_TIME , TimeUnit.MINUTES);

			} else { // Should never happen since we're testing the class
						// earlier
				throw new ClassCastException(
						"instance must be a Runnable or a Callable: "
								+ instance.getClass().getName() + " - "
								+ instance);
			}
			success(job, instance, result, curQueue);
		} catch ( TimeoutException e ) {
			task.cancel(true);
			throw new IllegalStateException( "Worker did not execute in less than 30 minutes", e );
		} finally {
			this.currentJobStartTime = null;
			this.jedisPool.withJedis(new JedisAction() {

				@Override
				public void execute(Jedis jedis) {
					jedis.del(key(WORKER, getName()));
				}

			});

		}
	}

	/**
	 * Update the status in Redis on success.
	 * 
	 * @param job
	 *            the Job that succeeded
	 * @param runner
	 *            the materialized Job
	 * @param curQueue
	 *            the queue the Job came from
	 */
	private void success(final Job job, final Object runner,
			final Object result, final String curQueue) {

		this.jedisPool.withJedis(new JedisAction() {

			@Override
			public void execute(Jedis jedis) {
				jedis.incr(key(STAT, PROCESSED));
				jedis.incr(key(STAT, PROCESSED, getName()));
			}

		});

		this.fireEvent(JOB_SUCCESS, this, curQueue, job, runner, result, null);
	}

	public String getName() {
		return this.name;
	}

	public void addQueue(final String queueName) {
		if (queueName == null || "".equals(queueName)) {
			throw new IllegalArgumentException(
					"queueName must not be null or empty: " + queueName);
		}
		this.queueNames.add(queueName);
	}

	public void removeQueue(final String queueName, final boolean all) {
		if (queueName == null || "".equals(queueName)) {
			throw new IllegalArgumentException(
					"queueName must not be null or empty: " + queueName);
		}
		if (all) { // Remove all instances
			boolean tryAgain = true;
			while (tryAgain) {
				tryAgain = this.queueNames.remove(queueName);
			}
		} else { // Only remove one instance
			this.queueNames.remove(queueName);
		}
	}

	public void removeAllQueues() {
		this.queueNames.clear();
	}

	public void setQueues(final Collection<String> queues) {
		WorkerUtils.checkQueues(queues);
		this.queueNames.clear();
		this.queueNames.addAll((queues == ALL_QUEUES) // Using object equality
														// on purpose
		? this.getQueuesFromJedis() // Like '*' in other clients
				: queues);
	}

	public Collection<String> getQueuesFromJedis() {

		return this.jedisPool
				.withJedis(new JedisResultAction<Collection<String>>() {

					@Override
					public Collection<String> execute(Jedis jedis) {

						return jedis.smembers(key(QUEUES));
					}

				});

	}

	/**
	 * Update the status in Redis on failure
	 * 
	 * @param ex
	 *            the Exception that occured
	 * @param job
	 *            the Job that failed
	 * @param curQueue
	 *            the queue the Job came from
	 */
	private void failure(final Exception ex, final Job job,
			final String curQueue) {

		try {
			this.failureBackend.onFailure(this, job, curQueue, ex);
		} catch (Exception e) {
			this.fireEvent(JOB_FAILURE, this, curQueue, job, null, null, e);
			log.error("Job failed", e);
			log.warn(
					"Error during serialization of failure payload for exception="
							+ ex + " job=" + job, e);
		}
		this.fireEvent(JOB_FAILURE, this, curQueue, job, null, null, ex);
	}

	/**
	 * Create and serialize a WorkerStatus.
	 * 
	 * @param queue
	 *            the queue the Job came from
	 * @param job
	 *            the Job currently being processed
	 * @return the JSON representation of a new WorkerStatus
	 * @throws IOException
	 *             if there was an error serializing the WorkerStatus
	 */
	private String statusMsg(final String queue, final Job job) {
		final WorkerStatus s = new WorkerStatus();
		s.setRunAt(new Date());
		s.setQueue(queue);
		s.setPayload(job);
		try {
			return ObjectMapperFactory.get().writeValueAsString(s);
		} catch (IOException e) {
			throw new RuntimeException(
					"Failed while generating status message for queue " + queue,
					e);
		}
	}

	/**
	 * Creates a unique name, suitable for use with Resque.
	 * 
	 * @return a unique name for this worker
	 */
	private String createName() {
		final StringBuilder sb = new StringBuilder(128);
		try {
			sb.append(InetAddress.getLocalHost().getHostName())
					.append(COLON)
					.append(ManagementFactory.getRuntimeMXBean().getName()
							.split("@")[0])
					// PID
					.append('-').append(this.workerId).append(COLON)
					.append(JAVA_DYNAMIC_QUEUES);
			for (final String queueName : this.queueNames) {
				sb.append(',').append(queueName);
			}
		} catch (UnknownHostException uhe) {
			throw new RuntimeException(uhe);
		}
		return sb.toString();
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

	/**
	 * Rename the current thread with the given message.
	 * 
	 * @param msg
	 *            the message to add to the thread name
	 */
	private void renameThread(final String msg) {
		Thread.currentThread().setName(this.threadNameBase + msg);
	}

	@Override
	public Date getCurrentJobStartTime() {
		return this.currentJobStartTime;
	}

	@Override
	public long getCurrentJobElapsedTime() {

		if (this.getCurrentJobStartTime() != null) {
			return new Date().getTime() - this.getCurrentJobStartTime().getTime();
		} else {
			return 0;
		}

	}

	@Override
	public String toString() {
		return this.namespace + COLON + WORKER + COLON + this.name;
	}
}
