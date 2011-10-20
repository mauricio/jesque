package net.greghaines.jesque.utils;

import net.greghaines.jesque.Config;
import net.greghaines.jesque.worker.JedisAction;
import net.greghaines.jesque.worker.JedisResultAction;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;

public class JedisPool extends redis.clients.jedis.JedisPool {

	private JedisPoolConfig poolConfig;
	private Config jesqueConfig;
	
	public JedisPool( Config jesqueConfig, JedisPoolConfig poolConfig) {
		super(poolConfig, jesqueConfig.getHost(), jesqueConfig.getPort());
		this.poolConfig = poolConfig;
		this.jesqueConfig = jesqueConfig;
	}
	
	public JedisPoolConfig getPoolConfig() {
		return poolConfig;
	}
	
	public Config getJesqueConfig() {
		return jesqueConfig;
	}
	
	public void withJedis( JedisAction action ) {
		
		Jedis jedis = null;
		
		try {
			jedis = this.getResource();
			action.execute( jedis );
		} catch ( Exception e ) {
			throw new RuntimeException(e);
		}
		finally {
			this.returnResource(jedis);
		}
		
	}
	
	public <T> T withJedis( JedisResultAction<T> action ) {
		
		Jedis jedis = null;
		T result = null;
		
		try {
			jedis = this.getResource();
			result = action.execute( jedis );
		} catch ( Exception e ) {
			throw new RuntimeException(e);
		}
		finally {
			this.returnResource(jedis);
		}		
		
		return result;
	}	
	
}
