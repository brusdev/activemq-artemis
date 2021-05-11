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
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.api.core.management.ActiveMQManagementProxy;
import org.apache.activemq.artemis.core.remoting.FailureListener;

import java.util.HashMap;
import java.util.Map;

public class BalancerTarget {
   private final String nodeID;
   private final TransportConfiguration connector;
   private final ServerLocator serverLocator;
   private final Map<String, Object> taskResults = new HashMap<>();

   private ClientSessionFactory sessionFactory;
   private ActiveMQManagementProxy managementProxy;

   public enum State {
      ATTACHED,
      CONNECTED,
      ACTIVATED,
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

   public ServerLocator getServerLocator() {
      return serverLocator;
   }

   public BalancerTarget(String nodeID, TransportConfiguration connector) {
      this.nodeID = nodeID;
      this.connector = connector;
      this.serverLocator = ActiveMQClient.createServerLocatorWithoutHA(connector);
   }

   public Object getTaskResult(String name) {
      return taskResults.get(name);
   }

   public void setTaskResult(String name, Object result) {
      taskResults.put(name, result);
   }

   public void attach() {
      state = State.ATTACHED;
   }

   public void connect() throws Exception {
      sessionFactory = serverLocator.createSessionFactory();
      sessionFactory.getConnection().addFailureListener(new FailureListener() {
         @Override
         public void connectionFailed(ActiveMQException exception, boolean failedOver) {
            try {
               disconnect();
            } catch (Exception e) {
               e.printStackTrace();
            }
         }

         @Override
         public void connectionFailed(ActiveMQException exception, boolean failedOver, String scaleDownTargetNodeID) {
            connectionFailed(exception, failedOver);
         }
      });

      managementProxy = new ActiveMQManagementProxy(sessionFactory);
      managementProxy.start();

      state = state == State.DETACHED ? State.DETACHED : State.CONNECTED;
   }

   public <T> T getAttribute(final Class<T> type, final String resourceName, final String attributeName) throws Exception {
      return managementProxy.getAttribute(type, resourceName, attributeName);
   }

   public <T> T invokeOperation(final Class<T> type, final String resourceName, final String operationName, final Object... operationArgs) throws Exception {
      return managementProxy.invokeOperation(type, resourceName, operationName, operationArgs);
   }

   public void active() {
      state = State.ACTIVATED;
   }

   public void disconnect() throws Exception {
      state = state == State.DETACHED ? State.DETACHED : State.ATTACHED;

      try {
         managementProxy.close();
      } catch (Exception e) {
         e.printStackTrace();
      }
      sessionFactory.close();
   }

   public void detach() {
      state = State.DETACHED;
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
