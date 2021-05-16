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
import org.apache.activemq.artemis.core.server.balancing.pools.DiscoveryAbstractPool;
import org.apache.activemq.artemis.core.server.balancing.pools.Pool;
import org.apache.activemq.artemis.core.server.balancing.pools.StaticAbstractPool;
import org.apache.activemq.artemis.core.server.balancing.targets.Target;
import org.apache.activemq.artemis.core.server.balancing.targets.TargetReference;
import org.apache.activemq.artemis.core.server.balancing.targets.TargetTask;

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
   private final Cache<String, Target> affinityCache;

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
         pool = new DiscoveryAbstractPool(server, scheduledExecutor, config.getDiscoveryGroupName());
      } else {
         pool = new StaticAbstractPool(server, scheduledExecutor, config.getStaticConnectors());
      }

      policy = loadPolicy(config.getPolicyConfiguration());

      pool.start();
   }

   private Policy loadPolicy(BalancerPolicyConfiguration policyConfig) throws ClassNotFoundException {
      Policy policy = PolicyFactory.createPolicyForName(policyConfig.getName());

      if (policy.getTargetTasks() != null) {
         for (TargetTask targetTask : policy.getTargetTasks()) {
            pool.addTargetTask(targetTask);
         }
      }

      if (policyConfig.getNext() != null) {
         policy.setNext(loadPolicy(policyConfig.getNext()));
      }

      return policy;
   }

   private Policy unloadPolicy(Policy policy) {
      if (policy.getTargetTasks() != null) {
         for (TargetTask targetTask : policy.getTargetTasks()) {
            pool.removeTargetTask(targetTask);
         }
      }

      if (policy.getNext() != null) {
         unloadPolicy(policy.getNext());
      }

      return policy;
   }

   @Override
   public void stop() throws Exception {
      unloadPolicy(policy);

      pool.stop();
   }

   @Override
   public boolean isStarted() {
      return false;
   }

   public TargetReference getTarget(String key) {
      Target target = affinityCache.getIfPresent(key);

      if (target != null && !pool.isTargetReady(target.getReference().getNodeID())) {
         target = null;

         affinityCache.invalidate(key);
      }

      if (target == null) {
         List<Target> targets = pool.getTargets();

         List<Target> selectedTargets = policy.selectTargets(targets, key);

         if (selectedTargets.size() > 0) {
            target = selectedTargets.get(0);
            affinityCache.put(key, target);
         }
      }

      return target.getReference();
   }
}
