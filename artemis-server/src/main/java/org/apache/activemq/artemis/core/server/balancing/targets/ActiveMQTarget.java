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

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.api.core.management.ActiveMQManagementProxy;
import org.apache.activemq.artemis.core.remoting.FailureListener;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.jboss.logging.Logger;

public class ActiveMQTarget extends AbstractTarget implements FailureListener {
   private static final Logger logger = Logger.getLogger(ActiveMQTarget.class);

   private boolean connected = false;

   private final ServerLocator serverLocator;

   private ClientSessionFactory sessionFactory;
   private RemotingConnection remotingConnection;
   private ActiveMQManagementProxy managementProxy;

   @Override
   public boolean isConnected() {
      return connected;
   }

   public ActiveMQTarget(TargetReference reference) {
      super(reference);

      serverLocator = ActiveMQClient.createServerLocatorWithoutHA(reference.getConnector());
   }


   @Override
   public void connect() throws Exception {
      sessionFactory = serverLocator.createSessionFactory();

      remotingConnection = sessionFactory.getConnection();
      remotingConnection.addFailureListener(this);

      managementProxy = new ActiveMQManagementProxy(sessionFactory.createSession(getUsername(), getPassword(),
         false, true, true, false, ActiveMQClient.DEFAULT_ACK_BATCH_SIZE).start());

      connected = true;

      fireConnectedEvent();
   }

   @Override
   public void disconnect() throws Exception {
      if (connected) {
         connected = false;

         managementProxy.close();

         remotingConnection.removeFailureListener(this);

         sessionFactory.close();

         fireDisconnectedEvent();
      }
   }

   @Override
   public void checkReadiness() throws Exception {
      if (!(boolean)getAttribute("broker", "Active", 3000)) {
         throw new IllegalStateException("Broker not active");
      }
   }

   public <T> T getAttribute(String resourceName, String attributeName, String t) throws Exception {
      return null;
   }

   @Override
   public Object getAttribute(String resourceName, String attributeName, int timeout) throws Exception {
      return managementProxy.getAttribute(resourceName, attributeName, timeout);
   }

   @Override
   public Object invokeOperation(String resourceName, String operationName, Object[] operationParams, int timeout) throws Exception {
      return managementProxy.invokeOperation(resourceName, operationName, operationParams, timeout);
   }

   @Override
   public void connectionFailed(ActiveMQException exception, boolean failedOver) {
      connectionFailed(exception, failedOver, null);
   }

   @Override
   public void connectionFailed(ActiveMQException exception, boolean failedOver, String scaleDownTargetNodeID) {
      try {
         if (connected) {
            disconnect();
         }
      } catch (Exception e) {
         logger.debug("Exception on disconnecting: ", e);
      }
   }
}
