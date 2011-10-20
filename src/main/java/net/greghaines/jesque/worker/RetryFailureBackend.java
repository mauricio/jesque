package net.greghaines.jesque.worker;

import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import net.greghaines.jesque.Config;
import net.greghaines.jesque.Job;
import net.greghaines.jesque.JobFailure;
import net.greghaines.jesque.json.ObjectMapperFactory;
import net.greghaines.jesque.utils.JedisPool;
import net.greghaines.jesque.utils.JesqueUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;

public class RetryFailureBackend implements FailureBackend {

	public static final String PENDING_RETRY_QUEUE = "pending_retry";
	public static final String NEXT_RETRY_AT = "next_retry_at";
	public static final String RETRY_DATA = "retry_data";
	public static final String RETRY_COUNT = "retry_count";
	public static final String LAST_FAILURE_AT = "last_failure_at";
	public static final String LAST_EXCEPTION = "last_exception";
	public static final String LAST_EXCEPTION_MESSAGE = "last_exception_message";
	
	private static final Logger log = LoggerFactory.getLogger(RetryFailureBackend.class);
	
	private FailureBackend finalFailureBackend;
	private Config config;
	private JedisPool pool;
	private int maxRetries = 3;
	private int backoffTime = 15;
	private Collection<String> retriableWorkers;
	
	public RetryFailureBackend( FailureBackend finalFailureBackend, Config config, JedisPool pool, String ... retryableWorkers ) {
		this.finalFailureBackend = finalFailureBackend;
		this.config = config;
		this.pool = pool;
		this.retriableWorkers = Arrays.asList( retryableWorkers );
	}

	public RetryFailureBackend( FailureBackend finalFailureBackend, Config config, JedisPool pool, int maxRetries, int backoffTime, String ... retryableWorkers ) {
		this( finalFailureBackend, config, pool, retryableWorkers );
		this.maxRetries = maxRetries;
		this.backoffTime = backoffTime;
	}
	
	@Override
	public void onFailure(final Worker worker, final Job job, final String queue,
			final Throwable exception) {

		log.warn( String.format( "Received failure from queue %s (%s) -> %s", queue, job.getClassName(), Arrays.asList( job.getArgs() ) ));
		
		Object message = job.getArgs()[0];
		
		if ( this.retriableWorkers.contains( job.getClassName().replaceAll( "::", "" ) ) && message instanceof Map<?,?> ) {
						
			final Map<String,Object> jobData = ( Map<String,Object> ) message;
			
			log.warn( String.format( "Found failed job at queue %s - %s", queue, jobData ) );
			
			Map<String,Object> retryData = ( Map<String,Object> ) jobData.get( RETRY_DATA );
			
			int count = 0;
			
			if ( retryData == null ) {
				retryData = new HashMap<String,Object>();
				retryData.put( RETRY_COUNT, count );
				jobData.put( RETRY_DATA , retryData);
			} else {
				count = (Integer) retryData.get( RETRY_COUNT );
				count++;
				retryData.put( RETRY_COUNT , count );
			}

			retryData.put( LAST_FAILURE_AT, System.currentTimeMillis() );
			retryData.put( LAST_EXCEPTION, JesqueUtils.getRootException( exception ).getClass().getName() );
			retryData.put( LAST_EXCEPTION_MESSAGE, JesqueUtils.getRootException( exception ).getMessage() );	
			
			if ( count <= this.maxRetries ) {			
				
				long nextRetry = System.currentTimeMillis() + ((count + 1) * TimeUnit.MINUTES.toMillis( this.backoffTime ) ) ;
				
				retryData.put( NEXT_RETRY_AT, nextRetry );
				
				log.warn( String.format("Retried %d times - next retry is at %s - %s", count, new Date( nextRetry ), jobData) );
				
				this.pool.withJedis( new JedisAction() {
					
					@Override
					public void execute(Jedis jedis) throws Exception {
						
						final JobFailure f = new JobFailure();
						f.setFailedAt(new Date());
						f.setWorker( worker.getName() );
						f.setQueue(queue);
						f.setPayload(job);
						f.setException( exception );
						
						jedis.rpush( key( PENDING_RETRY_QUEUE ) , ObjectMapperFactory.get().writeValueAsString(f)  );
						
					}
				});
				
			} else {
				
				log.warn( String.format("Job was retried %d times and will not be retried anymore, accepting it as failed - %s", count, jobData) );				
				this.finalFailureBackend.onFailure(worker, job, queue, exception);
				
			}
			
		
		} else {
			
			log.warn( String.format( "Not retrying job on queue %s (%s)", queue, job.getClassName() ));
			log.warn( String.format("Accepted class names are %s", this.retriableWorkers) );
			this.finalFailureBackend.onFailure(worker, job, queue, exception);
		}
		
	}
	
	protected String key(final String... parts)
	{
		return JesqueUtils.createKey(this.config.getNamespace(), parts);
	}

}
