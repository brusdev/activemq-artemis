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

@RunWith(Parameterized.class)
public class SingleTargetTest extends ClusterTestBase {
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


   public SingleTargetTest(String protocol, String policyName, boolean discovery) {
      this.protocol = protocol;
      this.policyName = policyName;
      this.discovery = discovery;
   }


   @Before
   public void setup() throws Exception {
      String groupAddress = ActiveMQTestBase.getUDPDiscoveryAddress();
      int groupPort = ActiveMQTestBase.getUDPDiscoveryPort();

      setupLiveServerWithDiscovery(0, groupAddress, groupPort, true, true, false);
      setupLiveServerWithDiscovery(1, groupAddress, groupPort, true, true, false);

      BrokerBalancerConfiguration brokerBalancerConfiguration = new BrokerBalancerConfiguration()
         .setName("simple-balancer").setPolicyConfiguration(new PolicyConfiguration().setName(policyName));

      if (discovery) {
         brokerBalancerConfiguration.setPoolConfiguration(new PoolConfiguration().setDiscoveryGroupName("dg1"));
      } else {
         TransportConfiguration connector = getServer(1).getConfiguration()
            .getConnectorConfigurations().values().stream().findFirst().get();

         getServer(0).getConfiguration().getConnectorConfigurations()
            .put(connector.getName(), connector);

         brokerBalancerConfiguration.setPoolConfiguration(new PoolConfiguration()
            .setStaticConnectors(Collections.singletonList(connector.getName())));
      }

      getServer(0).getConfiguration().setBalancerConfigurations(Collections.singletonList(brokerBalancerConfiguration));
      getServer(0).getConfiguration().getAcceptorConfigurations().iterator().next().getParams().put("redirect-to", "simple-balancer");
      getServer(0).getConfiguration().getAcceptorConfigurations().iterator().next().getParams().put("redirect-key", "USER_NAME");

      startServers(0, 1);
   }


   @Test
   public void testConnectionRedirect() throws Exception {
      ConnectionFactory connectionFactory = createFactory(protocol, TransportConstants.DEFAULT_HOST, TransportConstants.DEFAULT_PORT);

      try (Connection connection = connectionFactory.createConnection()) {
         connection.start();

         Assert.assertEquals(0, getServer(0).getConnectionCount());
         Assert.assertEquals(2, getServer(1).getConnectionCount());
      }
   }


   @Test
   public void testProduceAndConsumeAfterRedirect() throws Exception {
      final String messageText = "TEST";
      final SimpleString queueName = new SimpleString("TEST");

      ConnectionFactory connectionFactory = createFactory(protocol, TransportConstants.DEFAULT_HOST, TransportConstants.DEFAULT_PORT);

      try (Connection connection = connectionFactory.createConnection()) {
         connection.start();

         Assert.assertEquals(0, getServer(0).getConnectionCount());
         Assert.assertEquals(2, getServer(1).getConnectionCount());

         try (Session session = connection.createSession()) {
            Queue queue = session.createQueue(queueName.toString());

            try (MessageProducer producer = session.createProducer(queue)) {
               TextMessage message = session.createTextMessage(messageText);
               producer.send(message);
            }
         }
      }

      Assert.assertNull(getServer(0).getManagementService().getResource(ResourceNames.QUEUE + queueName));
      Assert.assertEquals(1, ((QueueControl)getServer(1).getManagementService().getResource(ResourceNames.QUEUE + queueName)).getMessageCount());

      try (Connection connection = connectionFactory.createConnection()) {
         connection.start();

         Assert.assertEquals(0, getServer(0).getConnectionCount());
         Assert.assertEquals(2, getServer(1).getConnectionCount());

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


   private ConnectionFactory createFactory(String protocol, String host, int port) {
      switch (protocol) {
         case AMQP_PROTOCOL:
            return new JmsConnectionFactory("failover:(amqp://" + host + ":" + port + ")");
         case CORE_PROTOCOL:
            ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://" + host + ":" + port + "?ha=true");
            connectionFactory.setReconnectAttempts(30);;
            return connectionFactory;
         default:
            throw new IllegalStateException("Unexpected value: " + protocol);
      }
   }

}
