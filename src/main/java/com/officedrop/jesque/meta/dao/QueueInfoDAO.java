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
package com.officedrop.jesque.meta.dao;

import java.util.List;

import com.officedrop.jesque.meta.QueueInfo;

public interface QueueInfoDAO
{
	List<String> getQueueNames();
	
	long getPendingCount();
	
	long getProcessedCount();
	
	List<QueueInfo> getQueueInfos();
	
	QueueInfo getQueueInfo(String name, int jobOffset, int jobCount);
	
	void removeQueue(String name);
}
