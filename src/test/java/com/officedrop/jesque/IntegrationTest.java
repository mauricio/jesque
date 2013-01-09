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

import com.fasterxml.jackson.core.JsonParseException;
import com.officedrop.jesque.worker.Worker;
import com.officedrop.jesque.worker.WorkerEvent;
import com.officedrop.jesque.worker.WorkerImpl;
import com.officedrop.jesque.worker.WorkerListener;
import com.officedrop.redis.failover.HostConfiguration;
import com.officedrop.redis.failover.jedis.CommonJedisFactory;
import com.officedrop.redis.failover.jedis.CommonsJedisPool;
import com.officedrop.redis.failover.jedis.JedisPool;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.*;

import static com.officedrop.jesque.TestUtils.*;
import static com.officedrop.jesque.utils.JesqueUtils.*;
import static com.officedrop.jesque.utils.ResqueConstants.*;
import static com.officedrop.jesque.worker.WorkerEvent.*;

/**
 * A.K.A. - The Whole Enchillada
 * 
 * @author Greg Haines
 */
public class IntegrationTest
{
	private static final Logger log = LoggerFactory.getLogger(IntegrationTest.class);
	private static final Config config = new ConfigBuilder().withJobPackage("com.officedrop.jesque").build();
	private static final String testQueue = "foo";
	private static final JedisPool pool = new CommonsJedisPool( new CommonJedisFactory( new HostConfiguration("localhost", 6379 )));
	
	@Before
	public void resetRedis()
	throws Exception
	{
		final Jedis jedis = createJedis(config);
		try
		{
			log.info("Resetting Redis for next test...");
			jedis.flushDB();
		}
		finally
		{
			jedis.quit();
		}
	}
	
	@Test
	public void jobSuccess()
	throws Exception
	{
		log.info("Running jobSuccess()...");
		assertSuccess(null);
	}
	
	@Test
	public void jobFailure()
	throws Exception
	{
		log.info("Running jobFailure()...");
		assertFailure(null);
	}
	
	@Test
	public void jobMixed()
	throws Exception
	{
		log.info("Running jobMixed()...");
		assertMixed(null);
	}
	
	@Test
	public void successInSpiteOfListenerFailPoll()
	{
		log.info("Running successInSpiteOfListenerFailPoll()...");
		assertSuccess(new FailingWorkerListener(), WORKER_POLL);
	}
	
	@Test
	public void successInSpiteOfListenerFailJob()
	{
		log.info("Running successInSpiteOfListenerFailJob()...");
		assertSuccess(new FailingWorkerListener(), JOB_PROCESS);
	}
	
	@Test
	public void successInSpiteOfListenerFailSuccess()
	{
		log.info("Running successInSpiteOfListenerFailSuccess()...");
		assertSuccess(new FailingWorkerListener(), JOB_SUCCESS);
	}
	
	@Test
	public void successInSpiteOfListenerFailAll()
	{
		log.info("Running successInSpiteOfListenerFailAll()...");
		assertSuccess(new FailingWorkerListener(), WorkerEvent.values());
	}
	
	@Test
	public void failureInSpiteOfListenerFailError()
	{
		log.info("Running failureInSpiteOfListenerFailError()...");
		assertFailure(new FailingWorkerListener(), WORKER_ERROR);
	}
	
	@Test
	public void failureInSpiteOfListenerFailAll()
	{
		log.info("Running failureInSpiteOfListenerFailAll()...");
		assertFailure(new FailingWorkerListener(), WorkerEvent.values());
	}
	
	@Test
	public void mixedInSpiteOfListenerFailAll()
	{
		log.info("Running mixedInSpiteOfListenerFailAll()...");
		assertMixed(new FailingWorkerListener(), WorkerEvent.values());
	}
	
	@Test
	public void checkQueuesAsRubyResqueDoes() throws Exception {
		final List<String> queuesPolled = new ArrayList<String>();
		final LinkedList<String> queuesWithJobs = new LinkedList<String>( Arrays.asList( "queue_1", "queue_2", "queue_1", "queue_3", "queue_3" ) );
		final Worker worker = new WorkerImpl(config, Arrays.asList( "queue_1", "queue_2", "queue_3" ), pool) {
			
			@Override
			protected boolean pollFromQueue(String queue)
					throws InterruptedException, JsonParseException,
					IOException {
								
				if ( queuesWithJobs.isEmpty() ) {
					end(false);
					return false;
				}
				
				queuesPolled.add( queue );
				
				String currentQueue = queuesWithJobs.peek();
				
				if ( currentQueue.equals( queue ) ) {
					queuesWithJobs.poll();
					return true;
				} else {
					return false;
				}			
				
			}
			
		};
		
		
		Thread t = new Thread(worker);
		t.start();
		t.join();
		
		List<String> result = Arrays.asList( "queue_1", "queue_1", "queue_2", "queue_1", "queue_1", "queue_2", "queue_3", "queue_1", "queue_2", "queue_3" );
		
		Assert.assertEquals( result, queuesPolled );
		
	}
	
	@SuppressWarnings("unchecked")
	private static void assertSuccess(final WorkerListener listener, final WorkerEvent... events)
	{
		final Job job = new Job("TestAction", new Object[]{ 1, 2.3, true, "test", Arrays.asList("inner", 4.5)});
		
		doWork(Arrays.asList(job), Arrays.asList(TestAction.class), listener, events);
		
		final Jedis jedis = createJedis(config);
		try
		{
			Assert.assertEquals("1", jedis.get(createKey(config.getNamespace(), STAT, PROCESSED)));
			Assert.assertNull(jedis.get(createKey(config.getNamespace(), STAT, FAILED)));
		}
		finally
		{
			jedis.quit();
		}
	}

	@SuppressWarnings("unchecked")
	private static void assertFailure(final WorkerListener listener, final WorkerEvent... events)
	{
		final Job job = new Job("FailAction");
		
		doWork(Arrays.asList(job), Arrays.asList(FailAction.class), listener, events);
		
		final Jedis jedis = createJedis(config);
		try
		{
			Assert.assertEquals("1", jedis.get(createKey(config.getNamespace(), STAT, FAILED)));
			Assert.assertNull(jedis.get(createKey(config.getNamespace(), STAT, PROCESSED)));
		}
		finally
		{
			jedis.quit();
		}
	}
	
	@SuppressWarnings("unchecked")
	private static void assertMixed(final WorkerListener listener, final WorkerEvent... events)
	{
		final Job job1 = new Job("FailAction");
		final Job job2 = new Job("TestAction", new Object[]{ 1, 2.3, true, "test", Arrays.asList("inner", 4.5)});
		final Job job3 = new Job("FailAction");
		final Job job4 = new Job("TestAction", new Object[]{ 1, 2.3, true, "test", Arrays.asList("inner", 4.5)});
		
		doWork(Arrays.asList(job1, job2, job3, job4), Arrays.asList(FailAction.class, TestAction.class), listener, events);
		
		final Jedis jedis = createJedis(config);
		try
		{
			Assert.assertEquals("2", jedis.get(createKey(config.getNamespace(), STAT, FAILED)));
			Assert.assertEquals("2", jedis.get(createKey(config.getNamespace(), STAT, PROCESSED)));
		}
		finally
		{
			jedis.quit();
		}
	}
	
	private static void doWork(final List<Job> jobs, final Collection<? extends Class<? extends Runnable>> jobTypes,
			final WorkerListener listener, final WorkerEvent... events)
	{
		final Worker worker = new WorkerImpl(config, Arrays.asList(testQueue), pool);
		if (listener != null && events.length > 0)
		{
			worker.addListener(listener, events);
		}
		final Thread workerThread = new Thread(worker);
		workerThread.start();
		try
		{
			TestUtils.enqueueJobs(testQueue, jobs, config);
		}
		finally
		{
			TestUtils.stopWorker(worker, workerThread);
		}
	}	
	
	private static class FailingWorkerListener implements WorkerListener
	{
		public void onEvent(final WorkerEvent event, final Worker worker, final String queue,
				final Job job, final Object runner, final Object result, final Exception ex)
		{
			throw new RuntimeException("Listener FAIL");
		}
	}
}
