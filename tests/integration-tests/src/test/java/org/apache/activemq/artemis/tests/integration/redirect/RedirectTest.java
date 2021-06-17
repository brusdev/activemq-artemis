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

package org.apache.activemq.artemis.tests.integration.redirect;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import java.util.ArrayList;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.management.QueueControl;
import org.apache.activemq.artemis.api.core.management.ResourceNames;
import org.apache.activemq.artemis.core.config.balancing.BrokerBalancerConfiguration;
import org.apache.activemq.artemis.core.config.balancing.PolicyConfiguration;
import org.apache.activemq.artemis.core.config.balancing.PoolConfiguration;
import org.apache.activemq.artemis.core.server.balancing.policies.FirstElementPolicy;
import org.apache.activemq.artemis.core.server.balancing.policies.LeastConnectionsPolicy;
import org.apache.activemq.artemis.core.server.redirect.RedirectKey;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.tests.integration.cluster.distribution.ClusterTestBase;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.junit.Assert;
import org.junit.Test;

public class RedirectTest extends ClusterTestBase {








   protected final String groupAddress = ActiveMQTestBase.getUDPDiscoveryAddress();

   protected final int groupPort = ActiveMQTestBase.getUDPDiscoveryPort();

   @Test
   public void testSimple() throws Exception {
      final SimpleString queueName = new SimpleString("RedirectTestQueue");
      ArrayList<BrokerBalancerConfiguration> brokerBalancerConfigurations = new ArrayList<>();
      brokerBalancerConfigurations.add(new BrokerBalancerConfiguration().setName("simple-balancer").
         setPoolConfiguration(new PoolConfiguration().setDiscoveryGroupName("dg1")).
         setPolicyConfiguration(new PolicyConfiguration().setName(FirstElementPolicy.NAME)));

      setupLiveServerWithDiscovery(0, groupAddress, groupPort, true, true, false);
      setupLiveServerWithDiscovery(1, groupAddress, groupPort, true, true, false);
      setupLiveServerWithDiscovery(2, groupAddress, groupPort, true, true, false);

      getServer(0).getConfiguration().setBalancerConfigurations(brokerBalancerConfigurations);
      getServer(0).getConfiguration().getAcceptorConfigurations().iterator().next().getParams().put("redirect-to", "simple-balancer");
      getServer(0).getConfiguration().getAcceptorConfigurations().iterator().next().getParams().put("redirect-key", "USER_NAME");

      startServers(0, 1, 2);

      ConnectionFactory connectionFactory = createFactory(1, org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants.DEFAULT_HOST, org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants.DEFAULT_PORT + 0, "admin", "admin");

      try (Connection connection = connectionFactory.createConnection()) {
         connection.start();
         try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            javax.jms.Queue queue = session.createQueue(queueName.toString());
            try (MessageProducer producer = session.createProducer(queue)) {
               producer.setDeliveryMode(DeliveryMode.PERSISTENT);

               TextMessage msg1 = session.createTextMessage("hello");
               msg1.setIntProperty("mycount", 0);
               producer.send(msg1);

               if (getServer(1).getManagementService().getResource(ResourceNames.QUEUE + queueName) != null) {
                  stopServers(1);
               } else {
                  stopServers(2);
               }

               TextMessage msg2 = session.createTextMessage("hello");
               msg2.setIntProperty("mycount", 0);
               producer.send(msg2);
            }
         }
      }

      startServers(1, 2);

      QueueControl queueControl0 = (QueueControl)getServer(0).getManagementService().getResource(ResourceNames.QUEUE + queueName);
      QueueControl queueControl1 = (QueueControl)getServer(1).getManagementService().getResource(ResourceNames.QUEUE + queueName);
      QueueControl queueControl2 = (QueueControl)getServer(2).getManagementService().getResource(ResourceNames.QUEUE + queueName);

      Assert.assertNull(queueControl0);
      Assert.assertTrue(queueControl1 == null ^ queueControl2 == null);
      if (queueControl1 != null) {
         Assert.assertEquals(1, queueControl1.countMessages());
      } else {
         Assert.assertEquals(1, queueControl2.countMessages());
      }

      try (Connection connection = connectionFactory.createConnection()) {
         connection.start();
         try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            javax.jms.Queue queue = session.createQueue(queueName.toString());

            try (MessageConsumer consumer = session.createConsumer(queue)) {

               TextMessage message = (TextMessage) consumer.receive(1000);
               Assert.assertNotNull(message);
               Assert.assertEquals(0, message.getIntProperty("mycount"));
               Assert.assertEquals("hello", message.getText());
            }
         }
      }
   }

   @Test
   public void testLeastConnections() throws Exception {
      final SimpleString queueName1 = new SimpleString("RedirectTestQueue1");
      final SimpleString queueName2 = new SimpleString("RedirectTestQueue2");
      ArrayList<BrokerBalancerConfiguration> brokerBalancerConfigurations = new ArrayList<>();
      brokerBalancerConfigurations.add(new BrokerBalancerConfiguration().setName("simple-balancer").
         setPoolConfiguration(new PoolConfiguration().setDiscoveryGroupName("dg1").setQuorumSize(2)).
         setPolicyConfiguration(new PolicyConfiguration().setName(LeastConnectionsPolicy.NAME)));

      setupLiveServerWithDiscovery(0, groupAddress, groupPort, true, true, false);
      setupLiveServerWithDiscovery(1, groupAddress, groupPort, true, true, false);
      setupLiveServerWithDiscovery(2, groupAddress, groupPort, true, true, false);

      getServer(0).getConfiguration().setBalancerConfigurations(brokerBalancerConfigurations);
      getServer(0).getConfiguration().getAcceptorConfigurations().iterator().next().getParams().put("redirect-to", "simple-balancer");
      getServer(0).getConfiguration().getAcceptorConfigurations().iterator().next().getParams().put("redirect-key", RedirectKey.USER_NAME.name());

      startServers(0, 1, 2);

      //ActiveMQServerControl serverControl = (ActiveMQServerControl)getServer(0).getManagementService().getResource(ResourceNames.BROKER);
      //serverControl.addUser("user1", "user1", "AMQ", true);
      //serverControl.addUser("user2", "user2", "AMQ", true);

      ConnectionFactory connectionFactory1 = createFactory(1, org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants.DEFAULT_HOST, org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants.DEFAULT_PORT + 0, "user1", "user1");
      ConnectionFactory connectionFactory2 = createFactory(1, org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants.DEFAULT_HOST, org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants.DEFAULT_PORT + 0, "user2", "user2");

      Assert.assertEquals(0, getServer(0).getTotalConsumerCount());
      Assert.assertEquals(0, getServer(1).getTotalConsumerCount());
      Assert.assertEquals(0, getServer(2).getTotalConsumerCount());

      try (Connection connection1 = connectionFactory1.createConnection()) {
         connection1.start();
         try (Session session1 = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            javax.jms.Queue queue1 = session1.createQueue(queueName1.toString());
            try (MessageProducer producer1 = session1.createProducer(queue1)) {
               producer1.send(session1.createTextMessage(queueName1.toString()));

               try (Connection connection2 = connectionFactory2.createConnection()) {
                  connection2.start();
                  try (Session session2 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                     javax.jms.Queue queue2 = session2.createQueue(queueName2.toString());
                     try (MessageProducer producer2 = session2.createProducer(queue2)) {
                        producer2.send(session2.createTextMessage(queueName2.toString()));
                     }
                  }
               }
            }
         }
      }

      Assert.assertEquals(0, getServer(0).getTotalConsumerCount());
      Assert.assertEquals(0, getServer(1).getTotalConsumerCount());
      Assert.assertEquals(0, getServer(2).getTotalConsumerCount());

      try (Connection connection1 = connectionFactory1.createConnection()) {
         connection1.start();
         try (Session session1 = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            javax.jms.Queue queue1 = session1.createQueue(queueName1.toString());
            try (MessageConsumer consumer1 = session1.createConsumer(queue1)) {

               TextMessage message1 = (TextMessage)consumer1.receive();
               Assert.assertEquals(queueName1.toString(), message1.getText());

               try (Connection connection2 = connectionFactory2.createConnection()) {
                  connection2.start();
                  try (Session session2 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                     javax.jms.Queue queue2 = session2.createQueue(queueName2.toString());
                     try (MessageConsumer consumer2 = session2.createConsumer(queue2)) {
                        TextMessage message2 = (TextMessage)consumer2.receive();
                        Assert.assertEquals(queueName2.toString(), message2.getText());

                        Assert.assertEquals(0, getServer(0).getTotalConsumerCount());
                        Assert.assertEquals(1, getServer(1).getTotalConsumerCount());
                        Assert.assertEquals(1, getServer(2).getTotalConsumerCount());
                     }
                  }
               }
            }
         }
      }

      Assert.assertEquals(0, getServer(0).getTotalConsumerCount());
      Assert.assertEquals(0, getServer(1).getTotalConsumerCount());
      Assert.assertEquals(0, getServer(2).getTotalConsumerCount());
   }

   @Test
   public void testLocalTarget() throws Exception {
      final SimpleString queueName = new SimpleString("RedirectTestQueue");
      ArrayList<BrokerBalancerConfiguration> brokerBalancerConfigurations = new ArrayList<>();

      setupLiveServerWithDiscovery(0, groupAddress, groupPort, true, true, false);
      setupLiveServerWithDiscovery(1, groupAddress, groupPort, true, true, false);
      setupLiveServerWithDiscovery(2, groupAddress, groupPort, true, true, false);

      brokerBalancerConfigurations.add(new BrokerBalancerConfiguration().setName("simple-balancer").
         setPoolConfiguration(new PoolConfiguration().setDiscoveryGroupName("dg1").setLocalConnector(getServer(0).getConfiguration()
            .getConnectorConfigurations().values().stream().findFirst().get().getName())).
         setPolicyConfiguration(new PolicyConfiguration().setName(FirstElementPolicy.NAME)));

      getServer(0).getConfiguration().setBalancerConfigurations(brokerBalancerConfigurations);
      getServer(0).getConfiguration().getAcceptorConfigurations().iterator().next().getParams().put("redirect-to", "simple-balancer");
      getServer(0).getConfiguration().getAcceptorConfigurations().iterator().next().getParams().put("redirect-key", "USER_NAME");

      startServers(0, 1, 2);

      ConnectionFactory connectionFactory = createFactory(1, org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants.DEFAULT_HOST, org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants.DEFAULT_PORT + 0, "admin", "admin");

      try (Connection connection = connectionFactory.createConnection()) {
         connection.start();
         try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            javax.jms.Queue queue = session.createQueue(queueName.toString());
            try (MessageProducer producer = session.createProducer(queue)) {
               TextMessage msg1 = session.createTextMessage("hello");
               msg1.setIntProperty("mycount", 0);
               producer.send(msg1);
            }
         }
      }

      QueueControl queueControl0 = (QueueControl)getServer(0).getManagementService().getResource(ResourceNames.QUEUE + queueName);
      QueueControl queueControl1 = (QueueControl)getServer(1).getManagementService().getResource(ResourceNames.QUEUE + queueName);
      QueueControl queueControl2 = (QueueControl)getServer(2).getManagementService().getResource(ResourceNames.QUEUE + queueName);

      Assert.assertEquals(1, queueControl0.countMessages());
      Assert.assertNull(queueControl1);
      Assert.assertNull(queueControl2);

      try (Connection connection = connectionFactory.createConnection()) {
         connection.start();
         try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            javax.jms.Queue queue = session.createQueue(queueName.toString());

            try (MessageConsumer consumer = session.createConsumer(queue)) {

               TextMessage message = (TextMessage) consumer.receive(1000);
               Assert.assertNotNull(message);
               Assert.assertEquals(0, message.getIntProperty("mycount"));
               Assert.assertEquals("hello", message.getText());
            }
         }
      }
   }




   private ConnectionFactory createFactory(int protocol, String host, int port, String user, String password) {
      switch (protocol) {
         case 1: ActiveMQConnectionFactory coreCF = new ActiveMQConnectionFactory("tcp://" + host + ":" + port + "?ha=true");// core protocol
            coreCF.setUser(user);
            coreCF.setPassword(password);
            coreCF.setCompressLargeMessage(true);
            coreCF.setMinLargeMessageSize(10 * 1024);
            coreCF.setReconnectAttempts(30);
            return coreCF;
         case 2: return new JmsConnectionFactory("failover:(amqp://" + host + ":" + port + ")"); // amqp
         case 3: return new org.apache.activemq.ActiveMQConnectionFactory("tcp://" + host + ":" + port); // openwire
         default: return null;
      }
   }
}
