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
import org.apache.activemq.artemis.core.config.BalancerConfiguration;
import org.apache.activemq.artemis.core.config.BalancerPolicyConfiguration;
import org.apache.activemq.artemis.core.server.balancing.policies.FirstElementPolicy;
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
      ArrayList<BalancerConfiguration> balancerConfigurations = new ArrayList<>();
      balancerConfigurations.add(new BalancerConfiguration().setName("simple-balancer").setDiscoveryGroupName("dg1").
         setPolicyConfiguration(new BalancerPolicyConfiguration().setName(FirstElementPolicy.NAME)));

      setupLiveServerWithDiscovery(0, groupAddress, groupPort, true, true, false);
      setupLiveServerWithDiscovery(1, groupAddress, groupPort, true, true, false);
      setupLiveServerWithDiscovery(2, groupAddress, groupPort, true, true, false);

      getServer(0).getConfiguration().setBalancerConfigurations(balancerConfigurations);
      getServer(0).getConfiguration().getAcceptorConfigurations().iterator().next().getParams().put("redirect-to", "simple-balancer");
      getServer(0).getConfiguration().getAcceptorConfigurations().iterator().next().getParams().put("redirect-key", "USER_NAME");

      startServers(0, 1, 2);

      ConnectionFactory connectionFactory = createFactory(2, org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants.DEFAULT_HOST, org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants.DEFAULT_PORT + 0);

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

   private ConnectionFactory createFactory(int protocol, String host, int port) {
      switch (protocol) {
         case 1: ActiveMQConnectionFactory coreCF = new ActiveMQConnectionFactory("tcp://" + host + ":" + port + "?ha=true");// core protocol
            coreCF.setUser("admin");
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
