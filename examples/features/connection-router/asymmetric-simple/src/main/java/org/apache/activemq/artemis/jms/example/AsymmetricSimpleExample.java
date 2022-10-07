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

import org.apache.activemq.artemis.jms.client.ActiveMQJMSConnectionFactory;
import org.apache.activemq.artemis.util.ServerUtil;
import org.apache.qpid.jms.JmsConnectionFactory;


/**
 * This example demonstrates how incoming client connections are partitioned across two brokers
 * it uses AMQP to take advantage of the simple round-robin retry logic of the failover url scheme
 */
public class AsymmetricSimpleExample {

   public static void main(final String[] args) throws Exception {


      Process[] servers = new Process[2];

      try {
         for (int i = 0; i < args.length; i++) {
            servers[i] = ServerUtil.startServer(args[i], AsymmetricSimpleExample.class.getSimpleName() + i,
               "tcp://localhost:" + (61616 + i) + "?clientID=$.artemis.internal.router.client.test", 5000);
         }

         produce("tcp://localhost:61617?ha=true&reconnectAttempts=-1&initialConnectAttempts=-1");

         consume("tcp://localhost:61616");

         produce("(tcp://localhost:61616,tcp://localhost:61617)?ha=true&reconnectAttempts=-1&initialConnectAttempts=-1");

         consume("tcp://localhost:61616");

         produce("(tcp://localhost:61616,tcp://localhost:61617)?ha=true&reconnectAttempts=-1&initialConnectAttempts=-1");

         consume("(tcp://localhost:61616,tcp://localhost:61617)?ha=true&reconnectAttempts=-1&initialConnectAttempts=-1");

         ServerUtil.killServer(servers[0]);

         produce("(tcp://localhost:61616,tcp://localhost:61617)?ha=true&reconnectAttempts=-1&initialConnectAttempts=-1");

         consume("(tcp://localhost:61616,tcp://localhost:61617)?ha=true&reconnectAttempts=-1&initialConnectAttempts=-1");

      } finally {
         for (int i = 0; i < args.length; i++) {
            ServerUtil.killServer(servers[i]);
         }
      }
   }

   private static void produce(String uri) throws Exception {
      ConnectionFactory connectionFactory = new ActiveMQJMSConnectionFactory(uri);

      System.out.println("Connecting to " + uri);
      try (Connection connectionProducer = connectionFactory.createConnection()) {
         Session session = connectionProducer.createSession(false, Session.AUTO_ACKNOWLEDGE);

         Queue queue = session.createQueue("exampleQueue");
         MessageProducer sender = session.createProducer(queue);
         for (int i = 0; i < 10; i++) {
            TextMessage message = session.createTextMessage("Hello world n" + i);
            System.out.println("Sending message " + message.getText() );
            sender.send(message);
         }
      }
   }

   private static void consume(String uri) throws Exception {
      ConnectionFactory connectionFactory = new ActiveMQJMSConnectionFactory(uri);

      System.out.println("Connecting to " + uri);
      try (Connection connectionConsumer = connectionFactory.createConnection()) {
         connectionConsumer.start();
         Session session = connectionConsumer.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue queue = session.createQueue("exampleQueue");
         MessageConsumer consumer = session.createConsumer(queue);
         for (int i = 0; i < 10; i++) {
            TextMessage message = (TextMessage) consumer.receive(5000);
            System.out.println("Received message " + message.getText());
         }
      }
   }
}
