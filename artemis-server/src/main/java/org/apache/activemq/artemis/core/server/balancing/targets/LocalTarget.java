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

package org.apache.activemq.artemis.core.server.balancing.targets;

import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.management.ManagementService;

public class LocalTarget extends AbstractTarget {
   private final ActiveMQServer server;
   private final ManagementService managementService;

   public LocalTarget(ActiveMQServer server) {
      super(null);

      this.server = server;
      this.managementService = server.getManagementService();
   }

   @Override
   public boolean isLocal() {
      return true;
   }

   @Override
   public String getNodeID() {
      return server.getNodeID().toString();
   }

   @Override
   public boolean isConnected() {
      return true;
   }

   @Override
   public void connect() throws Exception {

   }

   @Override
   public void disconnect() throws Exception {

   }

   @Override
   public boolean checkReadiness() {
      return true;
   }

   @Override
   public Object getAttribute(String resourceName, String attributeName, int timeout) throws Exception {
      return managementService.getAttribute(resourceName, attributeName);
   }

   @Override
   public Object invokeOperation(String resourceName, String operationName, Object[] operationParams, int timeout) throws Exception {
      return managementService.invokeOperation(resourceName, operationName, operationParams);
   }
}
