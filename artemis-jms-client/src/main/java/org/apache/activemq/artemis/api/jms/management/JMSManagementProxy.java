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

package org.apache.activemq.artemis.api.jms.management;

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.JsonUtil;
import org.apache.activemq.artemis.api.core.management.ManagementHelper;
import org.apache.activemq.artemis.management.ManagementProxy;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.json.JsonArray;
import java.util.UUID;

public class JMSManagementProxy implements ManagementProxy, ExceptionListener {
   private final ConnectionFactory connectionFactory;

   private Connection connection;

   public JMSManagementProxy(ConnectionFactory connectionFactory) {
      this.connectionFactory = connectionFactory;
   }

   @Override
   public void connect() throws Exception {
      connection= connectionFactory.createConnection();

      connection.setExceptionListener(this);

      connection.start();
   }

   @Override
   public void disconnect() throws Exception {
      connection.close();
   }

   @Override
   public Object getAttribute(String resourceName, String attributeName, int timeout) throws Exception {
      return sendRequest(resourceName, ManagementHelper.HDR_ATTRIBUTE.toString(), attributeName, null, timeout);
   }

   @Override
   public Object invokeOperation(String resourceName, String operationName, Object[] operationParams, int timeout) throws Exception {
      return sendRequest(resourceName, ManagementHelper.HDR_OPERATION_NAME.toString(), operationName, operationParams, timeout);
   }

   @Override
   public void close() throws Exception {
      disconnect();
   }

   private Object sendRequest(String resourceName, String operationKey, String operationName, Object[] operationParams, int timeout) throws Exception {
      try (Session session = connection.createSession()) {
         Queue managementQueue = session.createQueue(ActiveMQDefaultConfiguration.getDefaultManagementAddress().toString());
         Queue managementReplyQueue = session.createQueue(ActiveMQDefaultConfiguration.getDefaultManagementAddress().toString() + "." + UUID.randomUUID().toString());

         try (MessageProducer producer = session.createProducer(managementQueue);
              MessageConsumer consumer = session.createConsumer(managementReplyQueue)) {

            TextMessage requestMessage = session.createTextMessage();
            requestMessage.setStringProperty(ManagementHelper.HDR_RESOURCE_NAME.toString(), resourceName);
            requestMessage.setStringProperty(operationKey, operationName);
            requestMessage.setText(operationParams != null ? JsonUtil.toJSONArray(operationParams).toString() : null);
            requestMessage.setJMSReplyTo(managementReplyQueue);
            producer.send(requestMessage);

            Message replyMessage = consumer.receive(timeout);

            if (!isOperationSucceeded(replyMessage)) {
               throw new Exception("Failed " + resourceName + "." + operationName + ". Reason: " + getResult(replyMessage));
            }

            return getResult(replyMessage);
         }
      }
   }

   private boolean isOperationSucceeded(Message message) throws JMSException {
      return message.propertyExists(ManagementHelper.HDR_OPERATION_SUCCEEDED.toString()) &&
         message.getBooleanProperty(ManagementHelper.HDR_OPERATION_SUCCEEDED.toString());
   }


   private Object getResult(Message message) throws Exception {
      Object[] res = getResults(message);
      return JsonUtil.convertJsonValue(res[0], null);
   }

   private Object[] getResults(Message message) throws Exception {
      JsonArray jsonArray = JsonUtil.readJsonArray(((TextMessage)message).getText());
      return JsonUtil.fromJsonArray(jsonArray);
   }

   @Override
   public void onException(JMSException exception) {


   }
}
