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

package org.apache.activemq.artemis.tests.integration.balancing;

import javax.jms.ConnectionFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.balancing.BrokerBalancerConfiguration;
import org.apache.activemq.artemis.core.config.balancing.PolicyConfiguration;
import org.apache.activemq.artemis.core.config.balancing.PoolConfiguration;
import org.apache.activemq.artemis.core.server.balancing.targets.TargetKey;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.tests.integration.cluster.distribution.ClusterTestBase;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.qpid.jms.JmsConnectionFactory;

public class BalancingTestBase extends ClusterTestBase {
   protected static final String AMQP_PROTOCOL = "AMQP";

   protected static final String CORE_PROTOCOL = "CORE";

   protected static final String BROKER_BALANCER_NAME = "bb1";

   protected static final String DEFAULT_CONNECTOR_NAME = "DEFAULT";

   protected static final String GROUP_ADDRESS = ActiveMQTestBase.getUDPDiscoveryAddress();

   protected static final int GROUP_PORT = ActiveMQTestBase.getUDPDiscoveryPort();

   protected static final int MULTIPLE_TARGETS = 3;


   protected TransportConfiguration getDefaultServerAcceptor(final int node) {
      return getServer(node).getConfiguration().getAcceptorConfigurations().stream().findFirst().get();
   }

   protected TransportConfiguration getDefaultServerConnector(final int node) {
      Map<String, TransportConfiguration> connectorConfigurations = getServer(node).getConfiguration().getConnectorConfigurations();
      TransportConfiguration connector = connectorConfigurations.get(DEFAULT_CONNECTOR_NAME);
      return connector != null ? connector : connectorConfigurations.values().stream().findFirst().get();
   }

   protected TransportConfiguration setupDefaultServerConnector(final int node) {
      TransportConfiguration defaultServerConnector = getDefaultServerConnector(node);

      if (!defaultServerConnector.getName().equals(DEFAULT_CONNECTOR_NAME)) {
         defaultServerConnector = new TransportConfiguration(defaultServerConnector.getFactoryClassName(),
            defaultServerConnector.getParams(), DEFAULT_CONNECTOR_NAME, defaultServerConnector.getExtraParams());

         getServer(node).getConfiguration().getConnectorConfigurations().put(DEFAULT_CONNECTOR_NAME, defaultServerConnector);
      }

      return defaultServerConnector;
   }

   protected void setupBalancerServerWithDiscovery(final int node, final TargetKey targetKey, final String policyName, final String localFilter, final boolean localTargetEnabled, final int quorumSize) {
      Configuration configuration = getServer(node).getConfiguration();
      BrokerBalancerConfiguration brokerBalancerConfiguration = new BrokerBalancerConfiguration().setName(BROKER_BALANCER_NAME);

      setupDefaultServerConnector(node);

      brokerBalancerConfiguration.setTargetKey(targetKey).setLocalFilter(localFilter)
         .setPoolConfiguration(new PoolConfiguration().setQuorumSize(quorumSize)
            .setLocalTargetEnabled(localTargetEnabled).setDiscoveryGroupName("dg1"))
         .setPolicyConfiguration(new PolicyConfiguration().setName(policyName));

      configuration.setBalancerConfigurations(Collections.singletonList(brokerBalancerConfiguration));

      TransportConfiguration acceptor = getDefaultServerAcceptor(node);
      acceptor.getParams().put("redirect-to", BROKER_BALANCER_NAME);
   }

   protected void setupBalancerServerWithStaticConnectors(final int node, final TargetKey targetKey, final String policyName, final String localFilter, final boolean localTargetEnabled, final int quorumSize, final int... targetNodes) {
      Configuration configuration = getServer(node).getConfiguration();
      BrokerBalancerConfiguration brokerBalancerConfiguration = new BrokerBalancerConfiguration().setName(BROKER_BALANCER_NAME);

      setupDefaultServerConnector(node);

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
      acceptor.getParams().put("redirect-to", BROKER_BALANCER_NAME);
   }

   protected ConnectionFactory createFactory(String protocol, String host, int port, String user, String password) {
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
