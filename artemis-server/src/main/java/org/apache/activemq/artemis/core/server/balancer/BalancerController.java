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
import org.apache.activemq.artemis.core.server.ActiveMQComponent;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.balancer.policies.BalancerPolicy;
import org.apache.activemq.artemis.core.server.balancer.pools.DiscoveryBalancerPool;
import org.apache.activemq.artemis.core.server.balancer.pools.BalancerPool;
import org.apache.activemq.artemis.core.server.balancer.pools.StaticBalancerPool;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class BalancerController implements ActiveMQComponent {

   private final ActiveMQServer server;
   private final BalancerConfiguration config;
   private final BalancerPolicy policy;
   private final Cache<String, BalancerTarget> cache;
   private final BalancerPool pool;

   public BalancerPool getPool() {
      return pool;
   }

   public BalancerController(final ActiveMQServer server, final BalancerConfiguration config) {
      this.server = server;
      this.config = config;

      policy = config.getPolicy();

      cache = CacheBuilder.newBuilder().expireAfterAccess(3000, TimeUnit.MILLISECONDS).build();

      if (config.getDiscoveryGroupName() != null) {
         pool = new DiscoveryBalancerPool(server, config.getDiscoveryGroupName());
      } else {
         pool = new StaticBalancerPool(server, config.getStaticConnectors());
      }
   }

   @Override
   public void start() throws Exception {
      pool.start();

      this.policy.load(this);
   }

   @Override
   public void stop() throws Exception {
      this.policy.unload();

      pool.stop();
   }

   @Override
   public boolean isStarted() {
      return false;
   }

   public BalancerTarget getTarget(String key) {
      try {
         return cache.get(key, new Callable<BalancerTarget>() {
            @Override
            public BalancerTarget call() throws Exception {
               return policy.selectTarget(key);
            }
         });
      } catch (ExecutionException e) {
         e.printStackTrace();
      }

      return null;
   }
}
