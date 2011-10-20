package net.greghaines.jesque.worker;

import net.greghaines.jesque.Job;

public interface RetryObserver {

	public void retryingJob( Worker worker, String queue, Job job, Throwable exception, int count );
	public void givenUpOnJob( Worker worker, String queue, Job job, Throwable exception, int count );
	
}