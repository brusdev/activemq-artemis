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

package org.apache.activemq.artemis.core.server.balancing;

import org.apache.activemq.artemis.api.core.DiscoveryGroupConfiguration;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.cluster.DiscoveryGroup;
import org.apache.activemq.artemis.core.config.balancing.BrokerBalancerConfiguration;
import org.apache.activemq.artemis.core.config.balancing.PolicyConfiguration;
import org.apache.activemq.artemis.core.config.balancing.PoolConfiguration;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.server.ActiveMQComponent;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.balancing.policies.Policy;
import org.apache.activemq.artemis.core.server.balancing.policies.PolicyFactory;
import org.apache.activemq.artemis.core.server.balancing.pools.DiscoveryGroupService;
import org.apache.activemq.artemis.core.server.balancing.pools.DiscoveryPool;
import org.apache.activemq.artemis.core.server.balancing.pools.DiscoveryService;
import org.apache.activemq.artemis.core.server.balancing.pools.Pool;
import org.apache.activemq.artemis.core.server.balancing.pools.StaticPool;
import org.apache.activemq.artemis.core.server.balancing.targets.CoreTargetFactory;
import org.apache.activemq.artemis.core.server.balancing.targets.TargetFactory;
import org.apache.activemq.artemis.core.server.balancing.targets.TargetTask;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

public final class BrokerBalancerManager implements ActiveMQComponent {
   private static final Logger logger = Logger.getLogger(BrokerBalancerManager.class);

   private final Configuration config;
   private final ActiveMQServer server;
   private final ScheduledExecutorService scheduledExecutor;

   private Map<String, BrokerBalancer> balancerControllers = new HashMap<>();


   public BrokerBalancerManager(final Configuration config, final ActiveMQServer server, ScheduledExecutorService scheduledExecutor) {
      this.config = config;
      this.server = server;
      this.scheduledExecutor = scheduledExecutor;
   }

   public void deploy() throws Exception {
      for (BrokerBalancerConfiguration balancerConfig : config.getBalancerConfigurations()) {
         deployBrokerBalancer(balancerConfig);
      }
   }

   public void deployBrokerBalancer(BrokerBalancerConfiguration config) throws Exception {
      Pool pool = deployPool(config.getPoolConfiguration());

      Policy policy = deployPolicy(config.getPolicyConfiguration(), pool);

      BrokerBalancer balancer = new BrokerBalancer(config.getName(), pool, policy, config.getAffinityTimeout(), server);

      balancerControllers.put(balancer.getName(), balancer);

      server.getManagementService().registerBrokerBalancer(balancer);
   }

   private Pool deployPool(PoolConfiguration config) throws Exception {
      Pool pool;
      TargetFactory targetFactory = new CoreTargetFactory();

      if (config.getDiscoveryGroupName() != null) {
         DiscoveryGroupConfiguration discoveryGroupConfiguration = server.getConfiguration().
            getDiscoveryGroupConfigurations().get(config.getDiscoveryGroupName());

         DiscoveryService discoveryService = new DiscoveryGroupService(server.getNodeID().toString(), config.getDiscoveryGroupName(),
            discoveryGroupConfiguration.getRefreshTimeout(), discoveryGroupConfiguration.getBroadcastEndpointFactory(), null);

         pool = new DiscoveryPool(targetFactory, scheduledExecutor, config.getCheckPeriod(), discoveryService);
      } else {
         Map<String, TransportConfiguration> connectorConfigurations =
            server.getConfiguration().getConnectorConfigurations();

         List<TransportConfiguration> staticConnectors = new ArrayList<>();
         for (String staticConnector : config.getStaticConnectors()) {
            staticConnectors.add(connectorConfigurations.get(staticConnector));
         }

         pool = new StaticPool(targetFactory, scheduledExecutor, config.getCheckPeriod(), staticConnectors);
      }

      pool.setUsername(config.getUsername());
      pool.setPassword(config.getPassword());

      return pool;
   }

   private Policy deployPolicy(PolicyConfiguration policyConfig, Pool pool) throws ClassNotFoundException {
      Policy policy = PolicyFactory.createPolicyForName(policyConfig.getName());

      if (policy.getTargetTasks() != null) {
         for (TargetTask targetTask : policy.getTargetTasks()) {
            pool.addTargetTask(targetTask);
         }
      }

      if (policyConfig.getNext() != null) {
         policy.setNext(deployPolicy(policyConfig.getNext(), pool));
      }

      return policy;
   }

   @Override
   public void start() throws Exception {
      for (BrokerBalancer brokerBalancer : balancerControllers.values()) {
         brokerBalancer.start();
      }
   }

   @Override
   public void stop() throws Exception {
      for (BrokerBalancer balancer : balancerControllers.values()) {
         balancer.stop();
         server.getManagementService().registerBrokerBalancer(balancer);
      }
   }

   @Override
   public boolean isStarted() {
      return false;
   }

   public BrokerBalancer getBalancer(String name) {
      return balancerControllers.get(name);
   }
}
