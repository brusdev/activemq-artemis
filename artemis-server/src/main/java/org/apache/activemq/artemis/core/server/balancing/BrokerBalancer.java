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
import org.apache.activemq.artemis.core.server.ActiveMQComponent;
import org.apache.activemq.artemis.core.server.balancing.policies.Policy;
import org.apache.activemq.artemis.core.server.balancing.pools.Pool;
import org.apache.activemq.artemis.core.server.balancing.targets.Target;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class BrokerBalancer implements ActiveMQComponent {

   private final String name;

   private final Pool pool;

   private final Policy policy;

   private final Cache<String, Target> cache;

   private volatile boolean started = false;

   public Pool getPool() {
      return pool;
   }

   public Policy getPolicy() {
      return policy;
   }

   public Cache<String, Target> getCache() {
      return cache;
   }

   public String getName() {
      return name;
   }

   @Override
   public boolean isStarted() {
      return started;
   }


   public BrokerBalancer(final String name, final Pool pool, final Policy policy, final int affinityTimeout) {
      this.name = name;

      this.pool = pool;

      this.policy = policy;

      this.cache = CacheBuilder.newBuilder().expireAfterAccess(affinityTimeout, TimeUnit.MILLISECONDS).build();
   }

   @Override
   public void start() throws Exception {
      pool.start();

      started = true;
   }

   @Override
   public void stop() throws Exception {
      started = false;

      pool.stop();
   }

   public Target getTarget(String key) {
      Target target = cache.getIfPresent(key);

      if (target != null && !pool.isTargetReady(target.getNodeID())) {
         target = null;

         //Todo check if keeping the target not ready until replacement
         cache.invalidate(key);
      }

      if (target == null) {
         List<Target> targets = pool.getTargets();

         target = policy.selectTarget(targets, key);

         if (target != null) {
            cache.put(key, target);
         }
      }

      return target;
   }
}
