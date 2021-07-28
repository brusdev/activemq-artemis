/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.jms.example;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;

/**
 * This example is demonstrating how incoming client connections are evely redirected from one broker towards
 * worker brokers.
 */
public class SymmetricRedirectExample {

   public static void main(final String[] args) throws Exception {

      ConnectionFactory connectionFactoryClient0Server0 = new ActiveMQConnectionFactory("tcp://localhost:61616?ha=true&reconnectAttempts=30&clientID=CLIENT_0");
      ConnectionFactory connectionFactoryClient1Server0 = new ActiveMQConnectionFactory("tcp://localhost:61616?ha=true&reconnectAttempts=30&clientID=CLIENT_1");
      ConnectionFactory connectionFactoryClient0Server1 = new ActiveMQConnectionFactory("tcp://localhost:61617?ha=true&reconnectAttempts=30&clientID=CLIENT_0");
      ConnectionFactory connectionFactoryClient1Server1 = new ActiveMQConnectionFactory("tcp://localhost:61617?ha=true&reconnectAttempts=30&clientID=CLIENT_1");

      /**
       * Step 1. Create a connection for producer0 and producer1, and send a few messages.
       * the server0 will redirect the connection of each producer to a different target brokers.
       */
      Connection connectionProducer0 = null;
      Connection connectionProducer1 = null;

      try {
         connectionProducer0 = connectionFactoryClient0Server0.createConnection();
         connectionProducer1 = connectionFactoryClient1Server0.createConnection();

         for (Connection connectionProducer : new Connection[] {connectionProducer0, connectionProducer1}) {
            Session session = connectionProducer.createSession(false, Session.AUTO_ACKNOWLEDGE);

            Queue queue = session.createQueue("exampleQueue" + connectionProducer.getClientID());
            MessageProducer sender = session.createProducer(queue);
            for (int i = 0; i < 100; i++) {
               TextMessage message = session.createTextMessage("Hello world n" + i + " - " + connectionProducer.getClientID());
               System.out.println("Sending message " + message.getText() + "/" + connectionProducer.getClientID());
               sender.send(message);
            }
         }
      } finally {
         if (connectionProducer0 != null) {
            connectionProducer0.close();
         }

         if (connectionProducer1 != null) {
            connectionProducer1.close();
         }
      }

      /**
       * Step 2. create a connection for consumer0 and consumer1, and receive a few messages.
       * the server0 will redirect the connection to the same target broker of the respective producer
       * because the consumer and the producer share the same clientID.
       */
      Connection connectionConsumer0 = null;
      Connection connectionConsumer1 = null;

      try {
         connectionConsumer0 = connectionFactoryClient0Server1.createConnection();
         connectionConsumer1 = connectionFactoryClient1Server1.createConnection();

         for (Connection connectionConsumer : new Connection[] {connectionConsumer0, connectionConsumer1}) {
            connectionConsumer.start();
            Session session = connectionConsumer.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("exampleQueue" + connectionConsumer.getClientID());
            MessageConsumer consumer = session.createConsumer(queue);
            for (int i = 0; i < 100; i++) {
               TextMessage message = (TextMessage) consumer.receive(5000);
               System.out.println("Received message " + message.getText() + "/" + connectionConsumer.getClientID());
            }
         }
      } finally {
         if (connectionConsumer0 != null) {
            connectionConsumer0.close();
         }

         if (connectionConsumer1 != null) {
            connectionConsumer1.close();
         }
      }
   }
}
