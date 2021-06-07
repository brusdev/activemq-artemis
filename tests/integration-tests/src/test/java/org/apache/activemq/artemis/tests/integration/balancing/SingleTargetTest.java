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
import org.apache.activemq.artemis.core.server.balancing.policies.DefaultPolicyFactory;
import org.apache.activemq.artemis.tests.integration.cluster.distribution.ClusterTestBase;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
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
         TransportConfiguration connector = getFirstServerConnector(1);

         getServer(0).getConfiguration().getConnectorConfigurations().put(connector.getName(), connector);

         brokerBalancerConfiguration.setPoolConfiguration(new PoolConfiguration()
            .setStaticConnectors(Collections.singletonList(connector.getName())));
      }

      startServers(0, 1);
   }

   @After
   public void dispose() throws Exception {
      stopServers(0, 1);
   }

   @Test
   public void testSimpleKey() throws Exception {
      BrokerBalancer balancer = getServer(0).getBalancerManager().getBalancer("simple-balancer");

      Wait.assertTrue(() -> balancer.getTarget("test") != null, 10000, 100);

      Assert.assertEquals(getFirstServerConnector(1), balancer.getTarget("test").getConnector());
   }

   protected TransportConfiguration getFirstServerConnector(final int node) {
      return getServer(node).getConfiguration().getConnectorConfigurations().values().stream().findFirst().get();
   }
}
