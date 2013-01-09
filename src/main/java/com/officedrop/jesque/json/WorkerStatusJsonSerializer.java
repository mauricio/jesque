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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.officedrop.jesque.WorkerStatus;

import java.io.IOException;

/**
 * A custom Jackson serializer for WorkerStatuses.
 * Needed because WorkerStatus uses Java-style property names and Resque does not.
 * 
 * @author Greg Haines
 */
public class WorkerStatusJsonSerializer extends JsonSerializer<WorkerStatus>
{
	@Override
	public void serialize(final WorkerStatus workerStatus, final JsonGenerator jgen, final SerializerProvider provider)
	throws IOException
	{
		jgen.writeStartObject();
		jgen.writeFieldName("run_at");
		ObjectMapperFactory.get().writeValue(jgen, workerStatus.getRunAt());
		jgen.writeStringField("queue", workerStatus.getQueue());
		jgen.writeFieldName("payload");
		ObjectMapperFactory.get().writeValue(jgen, workerStatus.getPayload());
		jgen.writeEndObject();
	}
}
