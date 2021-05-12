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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.activemq.artemis.core.config.BalancerConfiguration;
import org.apache.activemq.artemis.core.config.BalancerPolicyConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQComponent;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.balancing.policies.Policy;
import org.apache.activemq.artemis.core.server.balancing.policies.PolicyFactory;
import org.apache.activemq.artemis.core.server.balancing.pools.DiscoveryPool;
import org.apache.activemq.artemis.core.server.balancing.pools.Pool;
import org.apache.activemq.artemis.core.server.balancing.pools.StaticPool;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class BrokerBalancer implements ActiveMQComponent {

   private final String name;
   private final BalancerConfiguration config;
   private final ActiveMQServer server;
   private final ScheduledExecutorService scheduledExecutor;

   private Pool pool;
   private Policy policy;
   private final Cache<String, BrokerBalancerTarget> affinityCache;

   public String getName() {
      return name;
   }

   public BrokerBalancer(final BalancerConfiguration config, final ActiveMQServer server, final ScheduledExecutorService scheduledExecutor) {
      name = config.getName();

      this.config = config;
      this.server = server;
      this.scheduledExecutor = scheduledExecutor;

      affinityCache = CacheBuilder.newBuilder().expireAfterAccess(config.getAffinityTimeout(), TimeUnit.MILLISECONDS).build();
   }

   @Override
   public void start() throws Exception {
      if (config.getDiscoveryGroupName() != null) {
         pool = new DiscoveryPool(server, scheduledExecutor, config.getDiscoveryGroupName());
      } else {
         pool = new StaticPool(server, scheduledExecutor, config.getStaticConnectors());
      }

      policy = createPolicy(config.getPolicyConfiguration());

      pool.start();

      this.policy.load(this);
   }

   private Policy createPolicy(BalancerPolicyConfiguration policyConfig) throws ClassNotFoundException {
      Policy policy = PolicyFactory.policyForName(policyConfig.getName());

      if (policyConfig.getNext() != null) {
         policy.setNext(createPolicy(policyConfig.getNext()));
      }

      return policy;
   }

   @Override
   public void stop() throws Exception {
      policy.unload();

      pool.stop();
   }

   @Override
   public boolean isStarted() {
      return false;
   }

   public BrokerBalancerTarget getTarget(String key) {
      BrokerBalancerTarget target = affinityCache.getIfPresent(key);

      if (target != null && target.getState() != BrokerBalancerTarget.State.ACTIVATED) {
         target = null;
      }

      if (target == null) {
         List<BrokerBalancerTarget> targets = pool.getTargets(BrokerBalancerTarget.State.ACTIVATED);

         List<BrokerBalancerTarget> selectedTargets = policy.selectTargets(targets, key);

         if (selectedTargets.size() > 0) {
            target = selectedTargets.get(0);
            affinityCache.put(key, target);
         }
      }

      return target;
   }
}
