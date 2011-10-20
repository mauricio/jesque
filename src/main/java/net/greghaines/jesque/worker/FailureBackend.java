package net.greghaines.jesque.worker;

import net.greghaines.jesque.Job;

public interface FailureBackend {

	public void onFailure( Worker worker, Job job, String queue, Throwable exception );
	
}
