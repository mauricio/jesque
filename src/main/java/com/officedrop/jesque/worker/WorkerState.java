package com.officedrop.jesque.worker;

/**
 * Used by WorkerImpl to manage internal state.
 * 
 * @author Greg Haines
 */

public enum WorkerState
{
	/**
	 * The Worker has not started running.
	 */
	NEW,
	/**
	 * The Worker is currently running.
	 */
	RUNNING,
	/**
	 * The Worker has shutdown.
	 */
	SHUTDOWN,
	
	PAUSED,
	
	FAILED
}