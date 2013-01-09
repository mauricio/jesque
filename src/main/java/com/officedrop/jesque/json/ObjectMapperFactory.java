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
package com.officedrop.jesque.json;

import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.officedrop.jesque.Job;
import com.officedrop.jesque.JobFailure;
import com.officedrop.jesque.WorkerStatus;


/**
 * A helper that creates a fully-configured singleton ObjectMapper.
 * 
 * @author Greg Haines
 */
public final class ObjectMapperFactory
{
	private static final ObjectMapper mapper = new ObjectMapper();

	static
	{

        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);


        SimpleModule module = new SimpleModule("resque", new Version(1, 0, 0, "1", "0", "0"))
                .addDeserializer(Job.class, new JobJsonDeserializer())
                .addDeserializer(JobFailure.class, new JobFailureJsonDeserializer())
                .addDeserializer( WorkerStatus.class, new WorkerStatusJsonDeserializer() )
                .addSerializer( Job.class, new JobJsonSerializer() )
                .addSerializer( JobFailure.class, new JobFailureJsonSerializer() )
                .addSerializer( WorkerStatus.class, new WorkerStatusJsonSerializer() )
                ;

        mapper.registerModule(module);
	}
	
	/**
	 * @return a fully-configured ObjectMapper
	 */
	public static ObjectMapper get()
	{
		return mapper;
	}
	
	private ObjectMapperFactory(){} // Utility class
}
