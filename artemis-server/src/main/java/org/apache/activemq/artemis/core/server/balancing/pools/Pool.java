/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.core.server.balancing.pools;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQComponent;
import org.apache.activemq.artemis.core.server.balancing.targets.Target;
import org.apache.activemq.artemis.core.server.balancing.targets.TargetTask;

import java.util.List;

public interface Pool extends ActiveMQComponent {
   String getUsername();

   void setUsername(String username);

   String getPassword();

   void setPassword(String password);

   int getCheckPeriod();

   void setCheckPeriod(int checkPeriod);

   List<Target> getTargets();

   List<Target> getAllTargets();

   List<TargetTask> getTargetTasks();


   void addTarget(String nodeID, TransportConfiguration connector) throws Exception;

   boolean isTargetReady(String nodeId);

   Target getTarget(String nodeID);

   Target removeTarget(String nodeID) throws Exception;


   void addTargetTask(TargetTask task);

   void removeTargetTask(TargetTask task);
}
