package com.officedrop.jesque.worker;

import com.officedrop.jesque.Job;

public interface FailureBackend {

	public void onFailure( Worker worker, Job job, String queue, Throwable exception );
	
}
