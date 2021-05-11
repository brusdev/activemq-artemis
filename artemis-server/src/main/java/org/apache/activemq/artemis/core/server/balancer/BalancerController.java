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

package org.apache.activemq.artemis.core.server.balancer;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.activemq.artemis.core.config.BalancerConfiguration;
import org.apache.activemq.artemis.core.config.BalancerPolicyConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQComponent;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.balancer.policies.BalancerPolicy;
import org.apache.activemq.artemis.core.server.balancer.policies.BalancerPolicyFactory;
import org.apache.activemq.artemis.core.server.balancer.pools.DiscoveryBalancerPool;
import org.apache.activemq.artemis.core.server.balancer.pools.BalancerPool;
import org.apache.activemq.artemis.core.server.balancer.pools.StaticBalancerPool;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class BalancerController implements ActiveMQComponent {

   private final BalancerConfiguration config;
   private final ActiveMQServer server;
   private final ScheduledExecutorService scheduledExecutor;

   private BalancerPool pool;
   private BalancerPolicy policy;
   private final Cache<String, BalancerTarget> affinityCache;

   public BalancerPool getPool() {
      return pool;
   }

   public BalancerController(final BalancerConfiguration config, final ActiveMQServer server, final ScheduledExecutorService scheduledExecutor) {
      this.config = config;
      this.server = server;
      this.scheduledExecutor = scheduledExecutor;

      affinityCache = CacheBuilder.newBuilder().expireAfterAccess(config.getAffinityTimeout(), TimeUnit.MILLISECONDS).build();
   }

   @Override
   public void start() throws Exception {
      if (config.getDiscoveryGroupName() != null) {
         pool = new DiscoveryBalancerPool(server, scheduledExecutor, config.getDiscoveryGroupName());
      } else {
         pool = new StaticBalancerPool(server, scheduledExecutor, config.getStaticConnectors());
      }

      policy = createPolicy(config.getPolicyConfiguration());

      pool.start();

      this.policy.load(this);
   }

   private BalancerPolicy createPolicy(BalancerPolicyConfiguration policyConfig) throws ClassNotFoundException {
      BalancerPolicy policy = BalancerPolicyFactory.policyForName(policyConfig.getName());

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

   public BalancerTarget getTarget(String key) {
      BalancerTarget target = affinityCache.getIfPresent(key);

      if (target != null && target.getState() != BalancerTarget.State.READY) {
         target = null;
      }

      if (target == null) {
         List<BalancerTarget> targets = pool.getTargets(BalancerTarget.State.READY);

         List<BalancerTarget> selectedTargets = policy.selectTargets(targets, key);

         if (selectedTargets.size() > 0) {
            target = selectedTargets.get(0);
            affinityCache.put(key, target);
         }
      }

      return target;
   }
}
