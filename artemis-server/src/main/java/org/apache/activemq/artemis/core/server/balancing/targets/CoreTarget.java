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
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.api.core.management.ActiveMQManagementProxy;
import org.apache.activemq.artemis.core.remoting.FailureListener;
import org.jboss.logging.Logger;

public class CoreTarget extends AbstractTarget {
   private static final Logger logger = Logger.getLogger(Target.class);

   private boolean connected = false;

   private final ServerLocator serverLocator;
   private ClientSessionFactory sessionFactory;
   private ActiveMQManagementProxy managementProxy;

   @Override
   public TargetReference getReference() {
      return null;
   }

   @Override
   public boolean isConnected() {
      return connected;
   }

   public CoreTarget(TargetReference reference) {
      super(reference);
      this.serverLocator = ActiveMQClient.createServerLocatorWithoutHA(reference.getConnector());
   }


   @Override
   public void connect() throws Exception {
      sessionFactory = serverLocator.createSessionFactory();
      sessionFactory.getConnection().addFailureListener(new FailureListener() {
         @Override
         public void connectionFailed(ActiveMQException exception, boolean failedOver) {
            try {
               disconnect();
            } catch (Exception e) {
               logger.debug("Exception on disconnecting: ", e);
            }
         }

         @Override
         public void connectionFailed(ActiveMQException exception, boolean failedOver, String scaleDownTargetNodeID) {
            connectionFailed(exception, failedOver);
         }
      });

      managementProxy = new ActiveMQManagementProxy(sessionFactory);
      managementProxy.start();

      connected = true;
   }

   @Override
   public void disconnect() throws Exception {
      connected = false;

      try {
         managementProxy.close();
      } catch (Exception e) {
         e.printStackTrace();
      }
      sessionFactory.close();
   }

   @Override
   public void checkReadiness() throws Exception {
      if (!(boolean)getAttribute("broker", "Active")) {
         throw new IllegalStateException("Broker not active");
      }
   }

   public <T> T getAttribute(String resourceName, String attributeName, String t) throws Exception {
      return null;
   }

   @Override
   public Object getAttribute(String resourceName, String attributeName) throws Exception {
      return managementProxy.getAttribute(resourceName, attributeName);
   }

   @Override
   public Object invokeOperation(String resourceName, String operationName, Object... operationArgs) throws Exception {
      return managementProxy.invokeOperation(resourceName, operationName, operationArgs);
   }
}
