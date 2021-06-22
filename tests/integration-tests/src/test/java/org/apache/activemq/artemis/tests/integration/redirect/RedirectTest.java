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
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.management.QueueControl;
import org.apache.activemq.artemis.api.core.management.ResourceNames;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.balancing.BrokerBalancerConfiguration;
import org.apache.activemq.artemis.core.config.balancing.PolicyConfiguration;
import org.apache.activemq.artemis.core.config.balancing.PoolConfiguration;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.core.server.balancing.policies.ConsistentHashPolicy;
import org.apache.activemq.artemis.core.server.balancing.policies.FirstElementPolicy;
import org.apache.activemq.artemis.core.server.balancing.policies.LeastConnectionsPolicy;
import org.apache.activemq.artemis.core.server.balancing.policies.RoundRobinPolicy;
import org.apache.activemq.artemis.core.server.balancing.targets.TargetKey;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.tests.integration.cluster.distribution.ClusterTestBase;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class RedirectTest extends ClusterTestBase {
   private static final String AMQP_PROTOCOL = "AMQP";

   private static final String CORE_PROTOCOL = "CORE";

   private static final String DEFAULT_CONNECTOR_NAME = "DEFAULT";

   private static final String GROUP_ADDRESS = ActiveMQTestBase.getUDPDiscoveryAddress();

   private static final int GROUP_PORT = ActiveMQTestBase.getUDPDiscoveryPort();

   private static final int MULTIPLE_TARGETS = 3;

   @Parameterized.Parameters(name = "protocol: {0}, discovery: {1}")
   public static Collection<Object[]> data() {
      Collection<Object[]> data = new ArrayList<>();

      for (String protocol : Arrays.asList(new String[] {AMQP_PROTOCOL, CORE_PROTOCOL})) {
         for (boolean discovery : Arrays.asList(new Boolean[] {false, true})) {
            data.add(new Object[] {protocol, discovery});
         }
      }

      return data;
   }


   private final String protocol;

   private final boolean discovery;


   public RedirectTest(String protocol, boolean discovery) {
      this.protocol = protocol;

      this.discovery = discovery;
   }

   @Test
   public void testSimpleRedirect() throws Exception {
      final String queueName = "RedirectTestQueue";

      setupLiveServerWithDiscovery(0, GROUP_ADDRESS, GROUP_PORT, true, true, false);
      setupLiveServerWithDiscovery(1, GROUP_ADDRESS, GROUP_PORT, true, true, false);
      if (discovery) {
         setupBalancerServerWithDiscovery(0, TargetKey.USER_NAME, FirstElementPolicy.NAME, null, false, 1);
      } else {
         setupBalancerServerWithStaticConnectors(0, TargetKey.USER_NAME, FirstElementPolicy.NAME, null, false, 1, 1);
      }

      startServers(0, 1);

      getServer(0).createQueue(new QueueConfiguration(queueName).setRoutingType(RoutingType.ANYCAST));
      getServer(1).createQueue(new QueueConfiguration(queueName).setRoutingType(RoutingType.ANYCAST));

      QueueControl queueControl0 = (QueueControl)getServer(0).getManagementService()
         .getResource(ResourceNames.QUEUE + queueName);
      QueueControl queueControl1 = (QueueControl)getServer(1).getManagementService()
         .getResource(ResourceNames.QUEUE + queueName);

      Assert.assertEquals(0, queueControl0.countMessages());
      Assert.assertEquals(0, queueControl1.countMessages());

      ConnectionFactory connectionFactory = createFactory(protocol, TransportConstants.DEFAULT_HOST,
         TransportConstants.DEFAULT_PORT + 0, "admin", "admin");


      try (Connection connection = connectionFactory.createConnection()) {
         connection.start();
         try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            javax.jms.Queue queue = session.createQueue(queueName);
            try (MessageProducer producer = session.createProducer(queue)) {
               producer.send(session.createTextMessage("TEST"));
            }
         }
      }

      Assert.assertEquals(0, queueControl0.countMessages());
      Assert.assertEquals(1, queueControl1.countMessages());

      try (Connection connection = connectionFactory.createConnection()) {
         connection.start();
         try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            try (MessageConsumer consumer = session.createConsumer(session.createQueue(queueName))) {
               TextMessage message = (TextMessage) consumer.receive(1000);
               Assert.assertNotNull(message);
               Assert.assertEquals("TEST", message.getText());
            }
         }
      }

      Assert.assertEquals(0, queueControl0.countMessages());
      Assert.assertEquals(0, queueControl1.countMessages());
   }

   @Test
   public void testRoundRobinRedirect() throws Exception {
      testEvenlyRedirect(RoundRobinPolicy.NAME);
   }

   @Test
   public void testLeastConnectionsRedirect() throws Exception {
      testEvenlyRedirect(LeastConnectionsPolicy.NAME);
   }

   private void testEvenlyRedirect(final String policyName) throws Exception {
      final String queueName = "RedirectTestQueue";
      final int targets = MULTIPLE_TARGETS;
      int[] nodes = new int[targets + 1];
      int[] targetNodes = new int[targets];
      QueueControl[] queueControls = new QueueControl[targets + 1];

      nodes[0] = 0;
      setupLiveServerWithDiscovery(0, GROUP_ADDRESS, GROUP_PORT, true, true, false);
      for (int i = 0; i < targets; i++) {
         nodes[i + 1] = i + 1;
         targetNodes[i] = i + 1;
         setupLiveServerWithDiscovery(i + 1, GROUP_ADDRESS, GROUP_PORT, true, true, false);
      }

      if (discovery) {
         setupBalancerServerWithDiscovery(0, TargetKey.USER_NAME, policyName, null, false, targets);
      } else {
         setupBalancerServerWithStaticConnectors(0, TargetKey.USER_NAME, policyName, null, false, targets, 1, 2, 3);
      }

      startServers(nodes);

      for (int node : nodes) {
         getServer(node).createQueue(new QueueConfiguration(queueName).setRoutingType(RoutingType.ANYCAST));

         queueControls[node] = (QueueControl)getServer(node).getManagementService()
            .getResource(ResourceNames.QUEUE + queueName);

         Assert.assertEquals(0, queueControls[node].countMessages());
      }


      ConnectionFactory[] connectionFactories = new ConnectionFactory[targets];
      Connection[] connections = new Connection[targets];
      Session[] sessions = new Session[targets];

      for (int i = 0; i < targets; i++) {
         connectionFactories[i] = createFactory(protocol, TransportConstants.DEFAULT_HOST,
            TransportConstants.DEFAULT_PORT + 0, "user" + i, "user" + i);

         connections[i] = connectionFactories[i].createConnection();
         connections[i].start();

         sessions[i] = connections[i].createSession(false, Session.AUTO_ACKNOWLEDGE);
      }

      for (int i = 0; i < targets; i++) {
         try (MessageProducer producer = sessions[i].createProducer(sessions[i].createQueue(queueName))) {
            producer.send(sessions[i].createTextMessage("TEST" + i));
         }

         sessions[i].close();
         connections[i].close();
      }

      Assert.assertEquals(0, queueControls[0].countMessages());
      for (int targetNode : targetNodes) {
         Assert.assertEquals("Messages of node " + targetNode, 1, queueControls[targetNode].countMessages());
      }

      for (int i = 0; i < targets; i++) {
         try (Connection connection = connectionFactories[i].createConnection()) {
            connection.start();
            try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
               try (MessageConsumer consumer = session.createConsumer(session.createQueue(queueName))) {
                  TextMessage message = (TextMessage) consumer.receive(1000);
                  Assert.assertNotNull(message);
                  Assert.assertEquals("TEST" + i, message.getText());
               }
            }
         }
      }

      for (int node : nodes) {
         Assert.assertEquals(0, queueControls[node].countMessages());
      }
   }

   @Test
   public void testSymmetricRedirect() throws Exception {
      final String queueName = "RedirectTestQueue";

      setupLiveServerWithDiscovery(0, GROUP_ADDRESS, GROUP_PORT, true, true, false);
      setupLiveServerWithDiscovery(1, GROUP_ADDRESS, GROUP_PORT, true, true, false);
      if (discovery) {
         setupBalancerServerWithDiscovery(0, TargetKey.USER_NAME, ConsistentHashPolicy.NAME, "DEFAULT", true, 2);
         setupBalancerServerWithDiscovery(1, TargetKey.USER_NAME, ConsistentHashPolicy.NAME, "DEFAULT", true, 2);
      } else {
         setupBalancerServerWithStaticConnectors(0, TargetKey.USER_NAME, ConsistentHashPolicy.NAME, "DEFAULT", true, 2, 1);
         setupBalancerServerWithStaticConnectors(1, TargetKey.USER_NAME, ConsistentHashPolicy.NAME, "DEFAULT", true, 2, 0);
      }

      startServers(0, 1);

      Assert.assertTrue(getServer(0).getNodeID() != getServer(1).getNodeID());

      getServer(0).createQueue(new QueueConfiguration(queueName).setRoutingType(RoutingType.ANYCAST));
      getServer(1).createQueue(new QueueConfiguration(queueName).setRoutingType(RoutingType.ANYCAST));

      QueueControl queueControl0 = (QueueControl)getServer(0).getManagementService()
         .getResource(ResourceNames.QUEUE + queueName);
      QueueControl queueControl1 = (QueueControl)getServer(1).getManagementService()
         .getResource(ResourceNames.QUEUE + queueName);

      Assert.assertEquals(0, queueControl0.countMessages());
      Assert.assertEquals(0, queueControl1.countMessages());

      ConnectionFactory connectionFactory0 = createFactory(protocol, TransportConstants.DEFAULT_HOST,
         TransportConstants.DEFAULT_PORT + 0, "admin", "admin");


      try (Connection connection = connectionFactory0.createConnection()) {
         connection.start();
         try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            javax.jms.Queue queue = session.createQueue(queueName);
            try (MessageProducer producer = session.createProducer(queue)) {
               producer.send(session.createTextMessage("TEST"));
            }
         }
      }

      Assert.assertTrue((queueControl0.countMessages() == 0 && queueControl1.countMessages() == 1) ||
         (queueControl0.countMessages() == 1 && queueControl1.countMessages() == 0));

      Assert.assertTrue(getServer(0).getNodeID() != getServer(1).getNodeID());

      ConnectionFactory connectionFactory1 = createFactory(protocol, TransportConstants.DEFAULT_HOST,
         TransportConstants.DEFAULT_PORT + 1, "admin", "admin");

      try (Connection connection = connectionFactory1.createConnection()) {
         connection.start();
         try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            try (MessageConsumer consumer = session.createConsumer(session.createQueue(queueName))) {
               TextMessage message = (TextMessage) consumer.receive(1000);
               Assert.assertNotNull(message);
               Assert.assertEquals("TEST", message.getText());
            }
         }
      }

      Assert.assertEquals(0, queueControl0.countMessages());
      Assert.assertEquals(0, queueControl1.countMessages());
   }

   @Test
   public void testRedirectAfterFailure() throws Exception {
      final String queueName = "RedirectTestQueue";

      setupLiveServerWithDiscovery(0, GROUP_ADDRESS, GROUP_PORT, true, true, false);
      setupLiveServerWithDiscovery(1, GROUP_ADDRESS, GROUP_PORT, true, true, false);
      setupLiveServerWithDiscovery(2, GROUP_ADDRESS, GROUP_PORT, true, true, false);
      if (discovery) {
         setupBalancerServerWithDiscovery(0, TargetKey.USER_NAME, FirstElementPolicy.NAME, null, false, 1);
      } else {
         setupBalancerServerWithStaticConnectors(0, TargetKey.USER_NAME, FirstElementPolicy.NAME, null, false, 1, 1, 2);
      }

      startServers(0, 1, 2);

      getServer(0).createQueue(new QueueConfiguration(queueName).setRoutingType(RoutingType.ANYCAST));
      getServer(1).createQueue(new QueueConfiguration(queueName).setRoutingType(RoutingType.ANYCAST));
      getServer(2).createQueue(new QueueConfiguration(queueName).setRoutingType(RoutingType.ANYCAST));

      QueueControl queueControl0 = (QueueControl)getServer(0).getManagementService()
         .getResource(ResourceNames.QUEUE + queueName);
      QueueControl queueControl1 = (QueueControl)getServer(1).getManagementService()
         .getResource(ResourceNames.QUEUE + queueName);
      QueueControl queueControl2 = (QueueControl)getServer(2).getManagementService()
         .getResource(ResourceNames.QUEUE + queueName);

      Assert.assertEquals(0, queueControl0.countMessages());
      Assert.assertEquals(0, queueControl1.countMessages());
      Assert.assertEquals(0, queueControl2.countMessages());

      int failedNode;
      ConnectionFactory connectionFactory = createFactory(protocol, TransportConstants.DEFAULT_HOST,
         TransportConstants.DEFAULT_PORT + 0, "admin", "admin");


      try (Connection connection = connectionFactory.createConnection()) {
         connection.start();
         try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            javax.jms.Queue queue = session.createQueue(queueName);
            try (MessageProducer producer = session.createProducer(queue)) {
               producer.send(session.createTextMessage("TEST_BEFORE_FAILURE"));

               if (queueControl1.countMessages() > 0) {
                  failedNode = 1;
               } else {
                  failedNode = 2;
               }

               stopServers(failedNode);

               producer.send(session.createTextMessage("TEST_AFTER_FAILURE"));
            }
         }
      }

      startServers(failedNode);

      Assert.assertEquals(0, queueControl0.countMessages());
      Assert.assertEquals(1, queueControl1.countMessages());
      Assert.assertEquals(1, queueControl2.countMessages());

      try (Connection connection = connectionFactory.createConnection()) {
         connection.start();
         try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            try (MessageConsumer consumer = session.createConsumer(session.createQueue(queueName))) {
               TextMessage message = (TextMessage) consumer.receive(1000);
               Assert.assertNotNull(message);
               Assert.assertEquals("TEST_AFTER_FAILURE", message.getText());
            }
         }
      }

      Assert.assertEquals(0, queueControl0.countMessages());
      if (failedNode == 1) {
         Assert.assertEquals(1, queueControl1.countMessages());
         Assert.assertEquals(0, queueControl2.countMessages());
      } else {
         Assert.assertEquals(0, queueControl1.countMessages());
         Assert.assertEquals(1, queueControl2.countMessages());
      }
   }


   private TransportConfiguration getDefaultServerAcceptor(final int node) {
      return getServer(node).getConfiguration().getAcceptorConfigurations().stream().findFirst().get();
   }

   private TransportConfiguration getDefaultServerConnector(final int node) {
      Map<String, TransportConfiguration> connectorConfigurations = getServer(node).getConfiguration().getConnectorConfigurations();
      TransportConfiguration connector = connectorConfigurations.get(DEFAULT_CONNECTOR_NAME);
      return connector != null ? connector : connectorConfigurations.values().stream().findFirst().get();
   }

   private TransportConfiguration setupDefaultServerConnector(final int node) {
      TransportConfiguration defaultServerConnector = getDefaultServerConnector(node);

      if (!defaultServerConnector.getName().equals(DEFAULT_CONNECTOR_NAME)) {
         defaultServerConnector = new TransportConfiguration(defaultServerConnector.getFactoryClassName(),
            defaultServerConnector.getParams(), DEFAULT_CONNECTOR_NAME, defaultServerConnector.getExtraParams());

         getServer(node).getConfiguration().getConnectorConfigurations().put(DEFAULT_CONNECTOR_NAME, defaultServerConnector);
      }

      return defaultServerConnector;
   }

   private void setupBalancerServerWithDiscovery(final int node, final TargetKey targetKey, final String policyName, final String localFilter, final boolean localTargetEnabled, final int quorumSize) {
      Configuration configuration = getServer(node).getConfiguration();
      TransportConfiguration defaultServerConnector = setupDefaultServerConnector(node);
      BrokerBalancerConfiguration brokerBalancerConfiguration = new BrokerBalancerConfiguration().setName("bb1");

      brokerBalancerConfiguration.setTargetKey(targetKey).setLocalFilter(localFilter)
         .setPoolConfiguration(new PoolConfiguration().setQuorumSize(quorumSize)
            .setLocalTargetEnabled(localTargetEnabled).setDiscoveryGroupName("dg1"))
         .setPolicyConfiguration(new PolicyConfiguration().setName(policyName));

      configuration.setBalancerConfigurations(Collections.singletonList(brokerBalancerConfiguration));

      TransportConfiguration acceptor = getDefaultServerAcceptor(node);
      acceptor.getParams().put("redirect-to", "bb1");
   }

   private void setupBalancerServerWithStaticConnectors(final int node, final TargetKey targetKey, final String policyName, final String localFilter, final boolean localTargetEnabled, final int quorumSize, final int... targetNodes) {
      Configuration configuration = getServer(node).getConfiguration();
      TransportConfiguration defaultServerConnector = setupDefaultServerConnector(node);
      BrokerBalancerConfiguration brokerBalancerConfiguration = new BrokerBalancerConfiguration().setName("bb1");


      List<String> staticConnectors = new ArrayList<>();
      for (int targetNode : targetNodes) {
         TransportConfiguration connector = getDefaultServerConnector(targetNode);
         configuration.getConnectorConfigurations().put(connector.getName(), connector);
         staticConnectors.add(connector.getName());
      }

      brokerBalancerConfiguration.setTargetKey(targetKey).setLocalFilter(localFilter)
         .setPoolConfiguration(new PoolConfiguration().setQuorumSize(quorumSize)
            .setLocalTargetEnabled(localTargetEnabled).setStaticConnectors(staticConnectors))
         .setPolicyConfiguration(new PolicyConfiguration().setName(policyName));

      configuration.setBalancerConfigurations(Collections.singletonList(brokerBalancerConfiguration));

      TransportConfiguration acceptor = getDefaultServerAcceptor(node);
      acceptor.getParams().put("redirect-to", "bb1");
   }

   private ConnectionFactory createFactory(String protocol, String host, int port, String user, String password) {
      switch (protocol) {
         case CORE_PROTOCOL: ActiveMQConnectionFactory coreCF = new ActiveMQConnectionFactory("tcp://" + host + ":" + port + "?ha=true");// core protocol
            coreCF.setUser(user);
            coreCF.setPassword(password);
            coreCF.setCompressLargeMessage(true);
            coreCF.setMinLargeMessageSize(10 * 1024);
            coreCF.setReconnectAttempts(30);
            return coreCF;
         case AMQP_PROTOCOL: return new JmsConnectionFactory(user, password, "failover:(amqp://" + host + ":" + port + ")"); // amqp
         case "OPENWIRE": return new org.apache.activemq.ActiveMQConnectionFactory("tcp://" + host + ":" + port); // openwire
         default: return null;
      }
   }
}
