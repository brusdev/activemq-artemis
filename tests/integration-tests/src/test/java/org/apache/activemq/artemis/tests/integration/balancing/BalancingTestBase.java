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

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.config.balancing.BrokerBalancerConfiguration;
import org.apache.activemq.artemis.core.config.balancing.PolicyConfiguration;
import org.apache.activemq.artemis.core.config.balancing.PoolConfiguration;
import org.apache.activemq.artemis.core.server.balancing.BrokerBalancer;
import org.apache.activemq.artemis.core.server.balancing.policies.ConsistentHashPolicy;
import org.apache.activemq.artemis.core.server.balancing.policies.FirstElementPolicy;
import org.apache.activemq.artemis.core.server.balancing.policies.LeastConnectionsPolicy;
import org.apache.activemq.artemis.core.server.balancing.policies.RoundRobinPolicy;
import org.apache.activemq.artemis.core.server.balancing.targets.Target;
import org.apache.activemq.artemis.tests.integration.cluster.distribution.ClusterTestBase;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.apache.activemq.artemis.utils.Wait;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(Parameterized.class)
public abstract class BalancingTestBase extends ClusterTestBase {
   private static final int TARGETS = 5;
   private static final int WAIT_PERIOD = 100;

   private static final String BROKER_BALANCER_NAME = "SIMPLE";

   private static final int POOL_CHECK_PERIOD = 3000;
   private static final int POOL_QUORUM_SIZE = 2;

   private final boolean discovery;

   private String policyName;


   @Parameterized.Parameters(name = "discovery: {0}")
   public static Collection<Object[]> data() {
      //return Arrays.asList(new Object[][] {{true}, {false}});
      return Arrays.asList(new Object[][] {{false}});
   }

   abstract String getPolicyName();

   public BalancingTestBase(boolean discovery) {
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

      BrokerBalancerConfiguration brokerBalancerConfiguration = new BrokerBalancerConfiguration()
         .setName(BROKER_BALANCER_NAME).setPolicyConfiguration(new PolicyConfiguration().setName(getPolicyName()));

      if (discovery) {
         brokerBalancerConfiguration.setPoolConfiguration(new PoolConfiguration().setDiscoveryGroupName("dg1"));
      } else {
         List<String> staticConnector = new ArrayList<>();

         for (int i = 1; i <= TARGETS; i++) {
            TransportConfiguration connector = getFirstServerConnector(i);
            getServer(0).getConfiguration().getConnectorConfigurations().put(connector.getName(), connector);
            staticConnector.add(connector.getName());
         }

         brokerBalancerConfiguration.setPoolConfiguration(new PoolConfiguration().setStaticConnectors(staticConnector));
      }

      brokerBalancerConfiguration.getPoolConfiguration()
         .setCheckPeriod(POOL_CHECK_PERIOD)
         .setQuorumSize(POOL_QUORUM_SIZE);

      getServer(0).getConfiguration().setBalancerConfigurations(Collections.singletonList(brokerBalancerConfiguration));

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
   public void testTargetReadiness() throws Exception {
      final String key = "TEST";
      BrokerBalancer balancer = getServer(0).getBalancerManager().getBalancer(BROKER_BALANCER_NAME);

      Wait.assertTrue(() -> balancer.getTarget(key) != null, POOL_CHECK_PERIOD * 3, WAIT_PERIOD);

      Assert.assertTrue(balancer.getPool().getTargets().size() >= POOL_QUORUM_SIZE);

      Wait.assertEquals(TARGETS, () -> balancer.getPool().getTargets().size(), POOL_CHECK_PERIOD * 3, WAIT_PERIOD);

      for (int i = 1; i <= TARGETS; i++) {
         stopServers(i);

         if (i < TARGETS - POOL_QUORUM_SIZE) {
            Wait.assertEquals(TARGETS - i, () -> balancer.getPool().getTargets().size(), POOL_CHECK_PERIOD * 2, WAIT_PERIOD);

            Assert.assertTrue(balancer.getTarget(key) != null);
         } else {
            Wait.assertEquals(0, () -> balancer.getPool().getTargets().size(), POOL_CHECK_PERIOD * 2, WAIT_PERIOD);

            Assert.assertTrue(balancer.getTarget(key) == null);
         }
      }
   }

   @Test
   public void testFixedKey() throws Exception {
      final String key = "TEST";
      BrokerBalancer balancer = getServer(0).getBalancerManager().getBalancer(BROKER_BALANCER_NAME);

      Wait.assertTrue(() -> balancer.getTarget(key) != null, POOL_CHECK_PERIOD * 3, WAIT_PERIOD);

      if (FirstElementPolicy.NAME.equals(policyName)) {
         Target target = balancer.getTarget(key);

         for (int i = 0; i < TARGETS; i++) {
            Assert.assertEquals(target, balancer.getTarget(key));
         }
      } else if (RoundRobinPolicy.NAME.equals(policyName)) {
         List<Target> targets = new ArrayList<>();

         for (int i = 0; i < TARGETS; i++) {
            Assert.assertTrue(!targets.contains(balancer.getTarget(key)));
         }
      } else if (ConsistentHashPolicy.NAME.equals(policyName)) {
         Target target = balancer.getTarget(key);

         for (int i = 0; i < TARGETS; i++) {
            Assert.assertEquals(target, balancer.getTarget(key));
         }
      } else if (LeastConnectionsPolicy.NAME.equals(policyName)) {
         Target target = balancer.getTarget(key);

         for (int i = 0; i < TARGETS; i++) {
            Assert.assertEquals(target, balancer.getTarget(key));
         }
      }
   }

   @Test
   public void testRandomKey() throws Exception {
      BrokerBalancer balancer = getServer(0).getBalancerManager().getBalancer(BROKER_BALANCER_NAME);

      Wait.assertTrue(() -> balancer.getTarget(RandomUtil.randomString()) != null, POOL_CHECK_PERIOD * 3, WAIT_PERIOD);

      if (FirstElementPolicy.NAME.equals(policyName)) {
         Target target = balancer.getTarget(RandomUtil.randomString());

         for (int i = 0; i < TARGETS; i++) {
            Assert.assertEquals(target, balancer.getTarget(RandomUtil.randomString()));
         }
      } else if (RoundRobinPolicy.NAME.equals(policyName)) {
         List<Target> targets = new ArrayList<>();

         for (int i = 0; i < TARGETS; i++) {
            Assert.assertTrue(!targets.contains(balancer.getTarget(RandomUtil.randomString())));
         }
      } else if (ConsistentHashPolicy.NAME.equals(policyName)) {
         Map<Target, String> targets = new HashMap<>();
         for (int i = 0; i < 100 * TARGETS && targets.size() < TARGETS; i++) {
            String key = RandomUtil.randomString();
            targets.put(balancer.getTarget(key), key);
         }

         Assert.assertEquals(TARGETS, targets.size());
      } else if (LeastConnectionsPolicy.NAME.equals(policyName)) {
         Target target = balancer.getTarget(RandomUtil.randomString());

         for (int i = 0; i < TARGETS; i++) {
            Assert.assertEquals(target, balancer.getTarget(RandomUtil.randomString()));
         }
      }
   }

   protected TransportConfiguration getFirstServerConnector(final int node) {
      return getServer(node).getConfiguration().getConnectorConfigurations().values().stream().findFirst().get();
   }
}
