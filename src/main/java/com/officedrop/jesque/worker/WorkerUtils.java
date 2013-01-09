package com.officedrop.jesque.worker;

import java.util.Collection;
import java.util.concurrent.Callable;

public class WorkerUtils {

	/**
	 * Verify the given job types are all valid.
	 * 
	 * @param jobTypes
	 *            the given job types
	 */
	public static void checkJobTypes(
			final Collection<? extends Class<?>> jobTypes) {
		if (jobTypes == null) {
			throw new IllegalArgumentException("jobTypes must not be null");
		}
		for (final Class<?> jobType : jobTypes) {
			if (jobType == null) {
				throw new IllegalArgumentException(
						"jobType's members must not be null: " + jobTypes);
			}
			if (!(Runnable.class.isAssignableFrom(jobType))
					&& !(Callable.class.isAssignableFrom(jobType))) {
				throw new IllegalArgumentException(
						"jobType's members must implement either Runnable or Callable: "
								+ jobTypes);
			}
		}
	}	

	/**
	 * Verify that the given queues are all valid.
	 * 
	 * @param queues
	 *            the given queues
	 */
	public static void checkQueues(final Iterable<String> queues) {
		if (queues == null) {
			throw new IllegalArgumentException("queues must not be null");
		}
		for (final String queue : queues) {
			if (queue == null || "".equals(queue)) {
				throw new IllegalArgumentException(
						"queues' members must not be null: " + queues);
			}
		}
	}	
	
}
