package net.greghaines.jesque.worker;

import redis.clients.jedis.Jedis;

public interface JedisAction {

	public void execute( Jedis jedis ) throws Exception;
	
}
