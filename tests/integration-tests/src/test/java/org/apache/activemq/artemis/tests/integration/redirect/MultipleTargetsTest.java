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

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.management.QueueControl;
import org.apache.activemq.artemis.api.core.management.ResourceNames;
import org.apache.activemq.artemis.core.config.balancing.BrokerBalancerConfiguration;
import org.apache.activemq.artemis.core.config.balancing.PolicyConfiguration;
import org.apache.activemq.artemis.core.config.balancing.PoolConfiguration;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.core.server.balancing.policies.DefaultPolicyFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.tests.integration.cluster.distribution.ClusterTestBase;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

@RunWith(Parameterized.class)
public class MultipleTargetsTest extends ClusterTestBase {
   private static final int TARGETS = 5;

   private static final String AMQP_PROTOCOL = "AMQP";
   private static final String CORE_PROTOCOL = "CORE";

   private final String protocol;
   private final String policyName;
   private final boolean discovery;


   @Parameterized.Parameters(name = "protocol: {0}, policy {1}, discovery: {2}")
   public static Collection<Object[]> data() {
      Collection<Object[]> data = new ArrayList<>();

      DefaultPolicyFactory policyFactory = new DefaultPolicyFactory();
      for (String protocol : Arrays.asList(new String[] {AMQP_PROTOCOL, CORE_PROTOCOL})) {
         for (String policyName : policyFactory.getSupportedPolicies()) {
            for (boolean discovery : Arrays.asList(new Boolean[] {false, true})) {
               data.add(new Object[] {protocol, policyName, discovery});
            }
         }
      }

      return data;
   }


   public MultipleTargetsTest(String protocol, String policyName, boolean discovery) {
      this.protocol = protocol;
      this.policyName = policyName;
      this.discovery = discovery;
   }


   @Before
   public void setup() throws Exception {
      String groupAddress = ActiveMQTestBase.getUDPDiscoveryAddress();
      int groupPort = ActiveMQTestBase.getUDPDiscoveryPort();

      setupLiveServerWithDiscovery(0, groupAddress, groupPort, true, true, false);
      for (int i = 1; i <= TARGETS; i++) {
         setupLiveServerWithDiscovery(i, groupAddress, groupPort, true, true, false);
      }

      final String brokerBalancerName = "simple-balancer";
      BrokerBalancerConfiguration brokerBalancerConfiguration = new BrokerBalancerConfiguration()
         .setName(brokerBalancerName).setPolicyConfiguration(new PolicyConfiguration().setName(policyName));

      if (discovery) {
         brokerBalancerConfiguration.setPoolConfiguration(new PoolConfiguration().setDiscoveryGroupName("dg1"));
      } else {

         List<String> staticConnector = new ArrayList<>();


         for (int i = 1; i <= TARGETS; i++) {
            TransportConfiguration connector = getServer(i).getConfiguration()
               .getConnectorConfigurations().values().stream().findFirst().get();

            getServer(0).getConfiguration().getConnectorConfigurations()
               .put(connector.getName(), connector);

            staticConnector.add(connector.getName());
         }

         brokerBalancerConfiguration.setPoolConfiguration(new PoolConfiguration().setStaticConnectors(staticConnector));
      }

      getServer(0).getConfiguration().setBalancerConfigurations(Collections.singletonList(brokerBalancerConfiguration));
      getServer(0).getConfiguration().getAcceptorConfigurations().iterator().next().getParams().put("redirect-to", brokerBalancerName);
      getServer(0).getConfiguration().getAcceptorConfigurations().iterator().next().getParams().put("redirect-key", "USER_NAME");


      startServers(0);
      for (int i = 1; i <= TARGETS; i++) {
         startServers(i);
      }
   }

   @After
   public void dispose() throws Exception {
      stopServers(0);
      for (int i = 1; i <= TARGETS; i++) {
         stopServers(i);
      }
   }


   @Test
   public void testConnectionRedirect() throws Exception {
      ConnectionFactory connectionFactory = createFactory(protocol, TransportConstants.DEFAULT_HOST, TransportConstants.DEFAULT_PORT);

      try (Connection connection = connectionFactory.createConnection()) {
         connection.start();

         Assert.assertEquals(0, getServer(0).getConnectionCount());

         List<Integer> serverConnectionCounts = getServerConnectionCounts();
         Assert.assertTrue(serverConnectionCounts.stream().filter(count -> count == 2).count() == 1);
         Assert.assertTrue(serverConnectionCounts.stream().filter(count -> count <= 1).count() == TARGETS - 1);
      }
   }


   @Test
   public void testProduceAndConsumeAfterRedirect() throws Exception {
      final String messageText = "TEST";
      final SimpleString queueName = new SimpleString("TEST");

      int targetServerIndex;

      ConnectionFactory connectionFactory = createFactory(protocol, TransportConstants.DEFAULT_HOST, TransportConstants.DEFAULT_PORT);

      try (Connection connection = connectionFactory.createConnection()) {
         connection.start();

         List<Integer> serverConnectionCounts = getServerConnectionCounts();
         Assert.assertTrue(serverConnectionCounts.stream().filter(count -> count == 2).count() == 1);
         Assert.assertTrue(serverConnectionCounts.stream().filter(count -> count <= 1).count() == TARGETS - 1);

         targetServerIndex = serverConnectionCounts.get(2);

         try (Session session = connection.createSession()) {
            Queue queue = session.createQueue(queueName.toString());

            try (MessageProducer producer = session.createProducer(queue)) {
               TextMessage message = session.createTextMessage(messageText);
               producer.send(message);
            }
         }
      }

      Assert.assertNull(getServer(0).getManagementService().getResource(ResourceNames.QUEUE + queueName));
      for (int i = 1; i <= TARGETS; i++) {
         if (i == targetServerIndex) {
            Assert.assertEquals(1, ((QueueControl)getServer(i).getManagementService().getResource(ResourceNames.QUEUE + queueName)).getMessageCount());
         } else {
            Assert.assertNull(getServer(i).getManagementService().getResource(ResourceNames.QUEUE + queueName));
         }
      }

      try (Connection connection = connectionFactory.createConnection()) {
         connection.start();

         List<Integer> serverConnectionCounts = getServerConnectionCounts();
         Assert.assertTrue(serverConnectionCounts.stream().filter(count -> count == 2).count() == 1);
         Assert.assertTrue(serverConnectionCounts.stream().filter(count -> count <= 1).count() == TARGETS - 1);

         try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            Queue queue = session.createQueue(queueName.toString());

            try (MessageConsumer consumer = session.createConsumer(queue)) {
               TextMessage message = (TextMessage) consumer.receive(1000);
               Assert.assertNotNull(message);
               Assert.assertEquals(messageText, message.getText());
            }
         }
      }
   }

   private List<Integer> getServerConnectionCounts() {
      List<Integer> serverConnectionCounts = new ArrayList<>();

      for (int i = 1; i <= TARGETS; i++) {
         serverConnectionCounts.add(getServer(i).getConnectionCount());
      }

      return serverConnectionCounts;
   }

   private List<Long> getQueueMessageCounts(String queueName) {
      List<Long> queueMessageCounts = new ArrayList<>();

      for (int i = 1; i <= TARGETS; i++) {
         queueMessageCounts.add(((QueueControl)getServer(i).getManagementService().getResource(ResourceNames.QUEUE + queueName)).getMessageCount());
      }

      return queueMessageCounts;
   }

   private ConnectionFactory createFactory(String protocol, String host, int port) {
      switch (protocol) {
         case AMQP_PROTOCOL:
            return new JmsConnectionFactory("failover:(amqp://" + host + ":" + port + ")");
         case CORE_PROTOCOL:
            ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://" + host + ":" + port + "?ha=true");
            connectionFactory.setReconnectAttempts(30);
            return connectionFactory;
         default:
            throw new IllegalStateException("Unexpected value: " + protocol);
      }
   }

}
