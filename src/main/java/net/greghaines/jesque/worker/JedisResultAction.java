package net.greghaines.jesque.worker;

import redis.clients.jedis.Jedis;

public interface JedisResultAction<T> {

	public T execute( Jedis jedis ) throws Exception ;
	
	
}
