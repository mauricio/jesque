/*
 * Copyright 2011 Greg Haines
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.officedrop.jesque.meta.dao.impl;

import static com.officedrop.jesque.utils.ResqueConstants.COLON;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;

import com.officedrop.jesque.Config;
import com.officedrop.jesque.meta.KeyInfo;
import com.officedrop.jesque.meta.KeyType;
import com.officedrop.jesque.meta.dao.KeysDAO;
import com.officedrop.jesque.utils.PoolUtils;
import com.officedrop.jesque.utils.PoolUtils.PoolWork;

import redis.clients.jedis.Jedis;
import redis.clients.util.Pool;

public class KeysDAORedisImpl implements KeysDAO
{
	private static final Pattern newLinePattern = Pattern.compile("\r\n");
	private static final Pattern colonPattern = Pattern.compile(":");
	
	private final Config config;
	private final Pool<Jedis> jedisPool;

	public KeysDAORedisImpl(final Config config, final Pool<Jedis> jedisPool)
	{
		if (config == null)
		{
			throw new IllegalArgumentException("config must not be null");
		}
		if (jedisPool == null)
		{
			throw new IllegalArgumentException("jedisPool must not be null");
		}
		this.config = config;
		this.jedisPool = jedisPool;
	}
	
	public KeyInfo getKeyInfo(final String key)
	{
		return PoolUtils.doWorkInPoolNicely(this.jedisPool, new KeyDAOPoolWork(key));
	}
	
	public KeyInfo getKeyInfo(final String key, final int offset, final int count)
	{
		return PoolUtils.doWorkInPoolNicely(this.jedisPool, new KeyDAOPoolWork(key, offset, count));
	}

	public List<KeyInfo> getKeyInfos()
	{
		return PoolUtils.doWorkInPoolNicely(this.jedisPool, new PoolWork<Jedis,List<KeyInfo>>()
		{
			public List<KeyInfo> doWork(final Jedis jedis)
			throws Exception
			{
				final Set<String> keys = jedis.keys(KeysDAORedisImpl.this.config.getNamespace() + COLON + "*");
				final List<KeyInfo> keyInfos = new ArrayList<KeyInfo>(keys.size());
				for (final String key : keys)
				{
					keyInfos.add(new KeyDAOPoolWork(key).doWork(jedis));
				}
				Collections.sort(keyInfos);
				return keyInfos;
			}
		});
	}
	
	public Map<String,String> getRedisInfo()
	{
		return PoolUtils.doWorkInPoolNicely(this.jedisPool, new PoolWork<Jedis,Map<String,String>>()
		{
			public Map<String,String> doWork(final Jedis jedis)
			throws Exception
			{
				final Map<String,String> infoMap = new TreeMap<String,String>();
				final String infoStr = jedis.info();
				final String[] keyValueStrs = newLinePattern.split(infoStr);
				for (final String keyValueStr : keyValueStrs)
				{
					final String[] keyAndValue = colonPattern.split(keyValueStr, 2);
					if (keyAndValue.length == 1)
					{
						infoMap.put(keyAndValue[0], null);
					}
					else
					{
						infoMap.put(keyAndValue[0], keyAndValue[1]);
					}
				}
				return new LinkedHashMap<String,String>(infoMap);
			}	
		});
	}
	
	private static final class KeyDAOPoolWork implements PoolWork<Jedis,KeyInfo>
	{
		private final String key;
		private final int offset;
		private final int count;
		private final boolean doArrayValue;
		
		private KeyDAOPoolWork(final String key)
		{
			this.key = key;
			this.offset = -1;
			this.count = -1;
			this.doArrayValue = false;
		}

		private KeyDAOPoolWork(final String key, final int offset, final int count)
		{
			this.key = key;
			this.offset = offset;
			this.count = count;
			this.doArrayValue = true;
		}

		public KeyInfo doWork(final Jedis jedis)
		throws Exception
		{
			final KeyInfo keyInfo;
			final KeyType keyType = KeyType.getKeyTypeByValue(jedis.type(this.key));
			switch (keyType)
			{
			case HASH:
				keyInfo = new KeyInfo(this.key, keyType);
				keyInfo.setSize(jedis.hlen(this.key));
				if (this.doArrayValue)
				{
					final List<String> allFields = new ArrayList<String>(jedis.hkeys(this.key));
					if (this.offset >= allFields.size())
					{
						keyInfo.setArrayValue(new ArrayList<String>(1));
					}
					else
					{
						final int toIndex = (this.offset + this.count > allFields.size()) ? allFields.size() : (this.offset + this.count);
						final List<String> subFields = allFields.subList(this.offset, toIndex);
						final List<String> values = jedis.hmget(this.key, subFields.toArray(new String[subFields.size()]));
						final List<String> arrayValue = new ArrayList<String>(subFields.size());
						for (int i = 0; i < subFields.size(); i++)
						{
							arrayValue.add("{" + subFields.get(i) + "=" + values.get(i) + "}");
						}
						keyInfo.setArrayValue(arrayValue);
					}
				}
				break;
			case LIST:
				keyInfo = new KeyInfo(this.key, keyType);
				keyInfo.setSize(jedis.llen(this.key));
				if (this.doArrayValue)
				{
					keyInfo.setArrayValue(jedis.lrange(this.key, this.offset, this.offset + this.count));
				}
				break;
			case SET:
				keyInfo = new KeyInfo(this.key, keyType);
				keyInfo.setSize(jedis.scard(this.key));
				if (this.doArrayValue)
				{
					final List<String> allMembers = new ArrayList<String>(jedis.smembers(this.key));
					if (this.offset >= allMembers.size())
					{
						keyInfo.setArrayValue(new ArrayList<String>(1));
					}
					else
					{
						final int toIndex = (this.offset + this.count > allMembers.size()) ? allMembers.size() : (this.offset + this.count);
						keyInfo.setArrayValue(new ArrayList<String>(allMembers.subList(this.offset, toIndex)));
					}
				}
				break;
			case STRING:
				keyInfo = new KeyInfo(this.key, keyType);
				keyInfo.setSize(jedis.strlen(this.key));
				if (this.doArrayValue)
				{
					final List<String> arrayValue = new ArrayList<String>(1);
					arrayValue.add(jedis.get(this.key));
					keyInfo.setArrayValue(arrayValue);
				}
				break;
			case ZSET:
				keyInfo = new KeyInfo(this.key, keyType);
				keyInfo.setSize(jedis.zcard(this.key));
				if (this.doArrayValue)
				{
					keyInfo.setArrayValue(new ArrayList<String>(jedis.zrange(this.key, this.offset, this.offset + this.count)));
				}
				break;
			default:
				keyInfo = null;
				break;
			}
			return keyInfo;
		}
	}
}
