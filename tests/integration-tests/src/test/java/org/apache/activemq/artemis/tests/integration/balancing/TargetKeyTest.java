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

import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.core.server.balancing.policies.FirstElementPolicy;
import org.apache.activemq.artemis.core.server.balancing.policies.Policy;
import org.apache.activemq.artemis.core.server.balancing.policies.PolicyFactory;
import org.apache.activemq.artemis.core.server.balancing.policies.PolicyFactoryResolver;
import org.apache.activemq.artemis.core.server.balancing.targets.Target;
import org.apache.activemq.artemis.core.server.balancing.targets.TargetKey;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

@RunWith(Parameterized.class)
public class TargetKeyTest extends BalancingTestBase {

   private static final String MOCK_POLICY_NAME = "MOCK_POLICY";

   @Parameterized.Parameters(name = "protocol: {0}")
   public static Collection<Object[]> data() {
      Collection<Object[]> data = new ArrayList<>();

      for (String protocol : Arrays.asList(new String[] {AMQP_PROTOCOL, CORE_PROTOCOL, OPENWIRE_PROTOCOL})) {
         data.add(new Object[] {protocol});
      }

      return data;
   }


   private final String protocol;

   private final List<String> keys = new ArrayList<>();


   public TargetKeyTest(String protocol) {
      this.protocol = protocol;
   }

   @Before
   public void setup() throws Exception {
      PolicyFactoryResolver.getInstance().registerPolicyFactory(
         new PolicyFactory() {
            @Override
            public String[] getSupportedPolicies() {
               return new String[] {MOCK_POLICY_NAME};
            }

            @Override
            public Policy createPolicy(String policyName) {
               return new FirstElementPolicy(MOCK_POLICY_NAME) {
                  @Override
                  public Target selectTarget(List<Target> targets, String key) {
                     keys.add(key);
                     return super.selectTarget(targets, key);
                  }
               };
            }
         });
   }

   @Test
   public void testClientIDKey() throws Exception {
      setupLiveServerWithDiscovery(0, GROUP_ADDRESS, GROUP_PORT, true, true, false);
      setupBalancerServerWithDiscovery(0, TargetKey.CLIENT_ID, MOCK_POLICY_NAME, null, true, null, 1);
      startServers(0);

      ConnectionFactory connectionFactory = createFactory(protocol, false, TransportConstants.DEFAULT_HOST,
         TransportConstants.DEFAULT_PORT + 0, "test", null, null);

      keys.clear();

      try (Connection connection = connectionFactory.createConnection()) {
         connection.start();
      }

      Assert.assertEquals(1, keys.size());
      Assert.assertEquals("test", keys.get(0));
   }

   @Test
   public void testSNIHostKey() throws Exception {
      setupLiveServerWithDiscovery(0, GROUP_ADDRESS, GROUP_PORT, true, true, false);
      getDefaultServerAcceptor(0).getParams().put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
      getDefaultServerAcceptor(0).getParams().put(TransportConstants.KEYSTORE_PATH_PROP_NAME, "verified-localdomain-keystore.jks");
      getDefaultServerAcceptor(0).getParams().put(TransportConstants.KEYSTORE_PASSWORD_PROP_NAME, "secureexample");

      setupBalancerServerWithDiscovery(0, TargetKey.SNI_HOST, MOCK_POLICY_NAME, null, true, null, 1);
      startServers(0);

      ConnectionFactory connectionFactory = createFactory(protocol, true, "localhost.localdomain",
         TransportConstants.DEFAULT_PORT + 0, null, null, null);

      try (Connection connection = connectionFactory.createConnection()) {
         connection.start();
      }

      Assert.assertEquals(1, keys.size());
      Assert.assertEquals("localhost.localdomain", keys.get(0));
   }

   @Test
   public void testSourceIPKey() throws Exception {
      setupLiveServerWithDiscovery(0, GROUP_ADDRESS, GROUP_PORT, true, true, false);
      setupBalancerServerWithDiscovery(0, TargetKey.SOURCE_IP, MOCK_POLICY_NAME, null, true, null, 1);
      startServers(0);

      ConnectionFactory connectionFactory = createFactory(protocol, false, TransportConstants.DEFAULT_HOST,
         TransportConstants.DEFAULT_PORT + 0, null, null, null);

      try (Connection connection = connectionFactory.createConnection()) {
         connection.start();
      }

      Assert.assertEquals(1, keys.size());
      Assert.assertEquals(InetAddress.getLoopbackAddress().getHostAddress(), keys.get(0));
   }

   @Test
   public void testUserNameKey() throws Exception {
      setupLiveServerWithDiscovery(0, GROUP_ADDRESS, GROUP_PORT, true, true, false);
      setupBalancerServerWithDiscovery(0, TargetKey.USER_NAME, MOCK_POLICY_NAME, null, true, null, 1);
      startServers(0);

      ConnectionFactory connectionFactory = createFactory(protocol, false, TransportConstants.DEFAULT_HOST,
         TransportConstants.DEFAULT_PORT + 0, null, "admin", "admin");

      try (Connection connection = connectionFactory.createConnection()) {
         connection.start();
      }

      Assert.assertEquals(1, keys.size());
      Assert.assertEquals("admin", keys.get(0));
   }
}
