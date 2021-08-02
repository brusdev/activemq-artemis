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

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.MessageHandler;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.api.core.management.ActiveMQManagementProxy;
import org.apache.activemq.artemis.api.core.management.CoreNotificationType;
import org.apache.activemq.artemis.api.core.management.ManagementHelper;
import org.apache.activemq.artemis.api.core.management.ResourceNames;
import org.apache.activemq.artemis.core.remoting.FailureListener;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.utils.UUIDGenerator;
import org.jboss.logging.Logger;

public class ActiveMQTarget extends AbstractTarget implements FailureListener, MessageHandler {
   private static final Logger logger = Logger.getLogger(ActiveMQTarget.class);

   private boolean connected = false;

   private final ServerLocator serverLocator;

   private ClientSessionFactory sessionFactory;
   private RemotingConnection remotingConnection;
   private ActiveMQManagementProxy managementProxy;

   private ClientSession notificationSession;
   private ClientConsumer notificationConsumer;

   @Override
   public boolean isLocal() {
      return false;
   }

   @Override
   public boolean isConnected() {
      return connected;
   }


   public ActiveMQTarget(String serverID, TransportConfiguration connector, String nodeID) {
      super(serverID, connector, nodeID);

      serverLocator = ActiveMQClient.createServerLocatorWithoutHA(connector);
   }


   @Override
   public void connect() throws Exception {
      sessionFactory = serverLocator.createSessionFactory();

      remotingConnection = sessionFactory.getConnection();
      remotingConnection.addFailureListener(this);
      managementProxy = new ActiveMQManagementProxy(createSession().start());

      setupNotificationConsumer();

      connected = true;

      fireConnectedEvent();
   }

   private ClientSession createSession() throws ActiveMQException {
      return sessionFactory.createSession(getUsername(), getPassword(),
         false, true, true, false, ActiveMQClient.DEFAULT_ACK_BATCH_SIZE);
   }

   private void setupNotificationConsumer() throws Exception {
      SimpleString queueName = new SimpleString("notif." + UUIDGenerator.getInstance().generateStringUUID() + "." + getServerID());

      SimpleString filter = new SimpleString(ManagementHelper.HDR_NOTIFICATION_TYPE + "=" + "'" +
         CoreNotificationType.SESSION_CREATED +
         "' AND " +
         ManagementHelper.HDR_DISTANCE +
         "<1");

      notificationSession = createSession();

      notificationSession.createQueue(new QueueConfiguration(queueName).setAddress(ActiveMQDefaultConfiguration.getDefaultManagementNotificationAddress()).setFilterString(filter).setDurable(false).setTemporary(true));

      notificationConsumer = notificationSession.createConsumer(queueName);

      notificationConsumer.setMessageHandler(this);

      notificationSession.start();

      //invokeOperation(ResourceNames.BROKER, "sendSessionInfoToQueue", new Object[] { notificationQueueName.toString() }, null, 5000);
   }

   @Override
   public void disconnect() throws Exception {
      if (connected) {
         connected = false;

         notificationConsumer.close();

         notificationSession.close();

         managementProxy.close();

         remotingConnection.removeFailureListener(this);

         sessionFactory.close();

         fireDisconnectedEvent();
      }
   }

   @Override
   public boolean checkReadiness() {
      try {
         if (getNodeID() == null) {
            setNodeID(getAttribute(ResourceNames.BROKER, "NodeID", String.class, 3000));
         }

         return getAttribute(ResourceNames.BROKER, "Active", Boolean.class, 3000);
      } catch (Exception e) {
         logger.warn("Error on check readiness", e);
      }

      return false;
   }

   @Override
   public <T> T getAttribute(String resourceName, String attributeName, Class<T> attributeClass, int timeout) throws Exception {
      return managementProxy.getAttribute(resourceName, attributeName, attributeClass, timeout);
   }

   @Override
   public <T> T invokeOperation(String resourceName, String operationName, Object[] operationParams, Class<T> operationClass, int timeout) throws Exception {
      return managementProxy.invokeOperation(resourceName, operationName, operationParams, operationClass, timeout);
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

   @Override
   public void onMessage(ClientMessage message) {
      SimpleString type = message.getSimpleStringProperty(ManagementHelper.HDR_NOTIFICATION_TYPE);

      CoreNotificationType notificationType = CoreNotificationType.valueOf(type.toString());

      if (notificationType == CoreNotificationType.SESSION_CREATED) {
         SimpleString id = message.getSimpleStringProperty(ManagementHelper.HDR_SESSION_NAME);
         SimpleString remoteAddress = message.getSimpleStringProperty(ManagementHelper.HDR_REMOTE_ADDRESS);
         SimpleString sniHost = message.getSimpleStringProperty(ManagementHelper.HDR_SNI_HOST);
         SimpleString clientID = message.getSimpleStringProperty(ManagementHelper.HDR_CLIENT_ID);
         SimpleString username = message.getSimpleStringProperty(ManagementHelper.HDR_USER);

         fireSessionCreatedEvent(id.toString(),
            remoteAddress != null ? remoteAddress.toString() : null,
            sniHost != null ? sniHost.toString() : null,
            clientID != null ? clientID.toString() : null,
            username != null ? username.toString() : null);
      }
   }
}
