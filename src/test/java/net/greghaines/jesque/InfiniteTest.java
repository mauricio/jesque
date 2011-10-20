package net.greghaines.jesque;

import static net.greghaines.jesque.TestUtils.createJedis;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import net.greghaines.jesque.utils.JedisPool;
import net.greghaines.jesque.worker.Worker;
import net.greghaines.jesque.worker.WorkerImpl;

import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;

public class InfiniteTest
{
	private static final Config config = new ConfigBuilder().withJobPackage("net.greghaines.jesque").build();
	private static final JedisPool pool = new JedisPool( config, new JedisPoolConfig() );

	@Before
	public void resetRedis()
	throws Exception
	{
		final Jedis jedis = createJedis(config);
		try
		{
			jedis.flushDB();
		}
		finally
		{
			jedis.quit();
		}
	}
	
	@Test
	public void dummy(){}
	
	@SuppressWarnings("unchecked")
//	@Test
	public void dontStopNow()
	throws InterruptedException
	{
		for (int i = 0; i < 5; i++)
		{
			final List<Job> jobs = new ArrayList<Job>(30);
			for (int j = 0; j < 30; j++)
			{
				jobs.add(new Job("TestAction", new Object[]{j, 2.3, true, "test", Arrays.asList("inner", 4.5)}));
			}
			TestUtils.enqueueJobs("foo" + i, jobs, config);
			jobs.clear();
			for (int j = 0; j < 5; j++)
			{
				jobs.add(new Job("FailAction"));
			}
			TestUtils.enqueueJobs("bar", jobs, config);
		}
		final Worker worker = new WorkerImpl(config, Arrays.asList("foo0", "bar","baz"), pool);
		final Thread workerThread = new Thread(worker);
		workerThread.start();
		
		TestUtils.enqueueJobs("inf", Arrays.asList(new Job("InfiniteAction")), config);
		final Worker worker2 = new WorkerImpl(config, Arrays.asList("inf"), pool);
		final Thread workerThread2 = new Thread(worker2);
		workerThread2.start();
		
		workerThread.join();
	}
}
