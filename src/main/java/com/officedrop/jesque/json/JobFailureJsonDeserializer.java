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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.officedrop.jesque.Job;
import com.officedrop.jesque.JobFailure;
import com.officedrop.jesque.utils.JesqueUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;
import java.util.List;

/**
 * A custom Jackson deserializer for JobFailures.
 * Needed because JobFailure uses Java-style property names and Resque does not.
 * 
 * @author Greg Haines
 */
public class JobFailureJsonDeserializer extends StdDeserializer<JobFailure>
{
	private static final Logger log = LoggerFactory.getLogger( JobFailureJsonDeserializer.class );
	
	private static final TypeReference<List<String>> stringListTypeRef = new TypeReference<List<String>>(){};

    public JobFailureJsonDeserializer() {
        super(JobFailure.class);
    }

    @Override
	public JobFailure deserialize(final JsonParser jp, final DeserializationContext ctxt)
	throws IOException, JsonProcessingException
	{
		final JobFailure jobFailure = new JobFailure();
		String exception = null;
		String error = null;
		List<String> backtrace = null;
		while (jp.getCurrentToken() != JsonToken.END_OBJECT)
		{
			jp.nextToken();
			if ("worker".equals(jp.getText()))
			{
				jp.nextToken();
				jobFailure.setWorker(jp.readValueAs(String.class));
			}
			else if ("queue".equals(jp.getText()))
			{
				jp.nextToken();
				jobFailure.setQueue(jp.readValueAs(String.class));
			}
			else if ("payload".equals(jp.getText()))
			{
				jp.nextToken();
				jobFailure.setPayload(jp.readValueAs(Job.class));
			}
			else if ("exception".equals(jp.getText()))
			{
				jp.nextToken();
				exception = jp.readValueAs(String.class);
			}
			else if ("error".equals(jp.getText()))
			{
				jp.nextToken();
				error = jp.readValueAs(String.class);
			}
			else if ("backtrace".equals(jp.getText()))
			{
				jp.nextToken();
				backtrace = jp.<List<String>>readValueAs(stringListTypeRef);
			}
			else if ("failed_at".equals(jp.getText()))
			{
				jp.nextToken();
				jobFailure.setFailedAt(jp.readValueAs(Date.class));
			}
			else if ("retried_at".equals(jp.getText()))
			{
				jp.nextToken();
				jobFailure.setRetriedAt(jp.readValueAs(Date.class));
			}
			else if (jp.getCurrentToken() != JsonToken.END_OBJECT)
			{
				throw new JsonMappingException("Unexpected field for JobFailure: " + jp.getText(), jp.getCurrentLocation());
			}
		}
		if (exception != null)
		{
			try
			{
				jobFailure.setException(JesqueUtils.recreateThrowable(exception, error, backtrace));
			}
			catch (Exception e)
			{
				log.error( String.format("Failed to create the exception object from failure %s - %s - %s", exception, error, backtrace) );
			}
		}
		return jobFailure;
	}
}
