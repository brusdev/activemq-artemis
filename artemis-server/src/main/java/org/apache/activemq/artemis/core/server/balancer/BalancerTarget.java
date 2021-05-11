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

package org.apache.activemq.artemis.core.server.balancer;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.api.core.management.ActiveMQManagementProxy;

import java.util.HashMap;
import java.util.Map;

public class BalancerTarget {
   private final String nodeID;
   private final TransportConfiguration connector;
   private final ServerLocator serverLocator;
   private final ActiveMQManagementProxy managementProxy;
   private final Map<String, Object> taskResults = new HashMap<>();

   public enum State {
      ATTACHED,
      READY,
      DETACHED
   }

   private volatile BalancerTarget.State state = BalancerTarget.State.DETACHED;

   public String getNodeID() {
      return nodeID;
   }

   public TransportConfiguration getConnector() {
      return connector;
   }

   public State getState() {
      return state;
   }

   public void setState(State state) {
      this.state = state;
   }

   public ServerLocator getServerLocator() {
      return serverLocator;
   }

   public BalancerTarget(String nodeID, TransportConfiguration connector) {
      this.nodeID = nodeID;
      this.connector = connector;
      this.serverLocator = ActiveMQClient.createServerLocatorWithoutHA(connector);
      this.managementProxy = new ActiveMQManagementProxy(serverLocator, null, null);


   }

   public Object getTaskResult(String name) {
      return taskResults.get(name);
   }

   public void setTaskResult(String name, Object result) {
      taskResults.put(name, result);
   }

   public void connect() throws Exception {
      this.managementProxy.start();
   }

   public <T> T getAttribute(final Class<T> type, final String resourceName, final String attributeName) throws Exception {
      return managementProxy.getAttribute(type, resourceName, attributeName);
   }

   public <T> T invokeOperation(final Class<T> type, final String resourceName, final String operationName, final Object... operationArgs) throws Exception {
      return managementProxy.invokeOperation(type, resourceName, operationName, operationArgs);
   }


   public void disconnect() throws ActiveMQException {
      this.managementProxy.stop();
   }

   @Override
   public String toString() {
      StringBuilder stringBuilder = new StringBuilder(BalancerTarget.class.getSimpleName());
      stringBuilder.append("(nodeID=" + nodeID);
      stringBuilder.append(", connector=" + connector);
      stringBuilder.append(") ");
      return stringBuilder.toString();
   }
}
