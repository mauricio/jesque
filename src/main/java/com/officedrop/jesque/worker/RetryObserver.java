package com.officedrop.jesque.worker;

import com.officedrop.jesque.Job;

public interface RetryObserver {

	public void retryingJob( Worker worker, String queue, Job job, Throwable exception, int count );
	public void givenUpOnJob( Worker worker, String queue, Job job, Throwable exception, int count );
	
}