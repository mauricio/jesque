package net.greghaines.jesque.worker;

import net.greghaines.jesque.Job;


public abstract class BaseWorker implements Worker {

	private final WorkerListenerDelegate listenerDelegate = new WorkerListenerDelegate();
	
	public WorkerListenerDelegate getListenerDelegate() {
		return listenerDelegate;
	}
	
	protected void fireEvent(final WorkerEvent event, final Worker worker, final String queue, 
			final Job job, final Object runner, final Object result, final Exception ex)
	{
		this.listenerDelegate.fireEvent(event, worker, queue, job, runner, result, ex);
	}

	public void addListener(final WorkerListener listener) {
		this.listenerDelegate.addListener(listener);
	}

	public void addListener(final WorkerListener listener,
			final WorkerEvent... events) {
		this.listenerDelegate.addListener(listener, events);
	}

	public void removeListener(final WorkerListener listener) {
		this.listenerDelegate.removeListener(listener);
	}

	public void removeListener(final WorkerListener listener,
			final WorkerEvent... events) {
		this.listenerDelegate.removeListener(listener, events);
	}

	public void removeAllListeners() {
		this.listenerDelegate.removeAllListeners();
	}

	public void removeAllListeners(final WorkerEvent... events) {
		this.listenerDelegate.removeAllListeners(events);
	}	
	
}
