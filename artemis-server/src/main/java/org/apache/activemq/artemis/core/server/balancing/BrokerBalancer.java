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
import org.apache.activemq.artemis.core.server.balancing.targets.TargetKey;
import org.apache.activemq.artemis.core.server.balancing.targets.TargetKeyResolver;
import org.apache.activemq.artemis.core.server.balancing.targets.TargetListener;
import org.apache.activemq.artemis.spi.core.remoting.Connection;
import org.jboss.logging.Logger;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class BrokerBalancer implements ActiveMQComponent, TargetListener {
   private static final Logger logger = Logger.getLogger(BrokerBalancer.class);


   private final String name;

   private final TargetKey targetKey;

   private final TargetKeyResolver targetKeyResolver;

   private final Target localTarget;

   private final Pattern localTargetFilter;

   private final Pool pool;

   private final Policy policy;

   private final Cache<String, Target> cache;

   private volatile boolean started = false;

   public String getName() {
      return name;
   }

   public TargetKey getTargetKey() {
      return targetKey;
   }

   public Target getLocalTarget() {
      return localTarget;
   }

   public String getLocalTargetFilter() {
      return localTargetFilter != null ? localTargetFilter.pattern() : null;
   }

   public Pool getPool() {
      return pool;
   }

   public Policy getPolicy() {
      return policy;
   }

   public Cache<String, Target> getCache() {
      return cache;
   }

   @Override
   public boolean isStarted() {
      return started;
   }


   public BrokerBalancer(final String name, final TargetKey targetKey, final String targetKeyFilter, final Target localTarget, final String localTargetFilter, final Pool pool, final Policy policy, final int cacheTimeout) {
      this.name = name;

      this.targetKey = targetKey;

      this.targetKeyResolver = new TargetKeyResolver(targetKey, targetKeyFilter);

      this.localTarget = localTarget;

      this.localTargetFilter = localTargetFilter != null ? Pattern.compile(localTargetFilter) : null;

      this.pool = pool;

      this.policy = policy;

      this.cache = CacheBuilder.newBuilder().expireAfterAccess(cacheTimeout, TimeUnit.MILLISECONDS).build();
   }

   @Override
   public void start() throws Exception {
      pool.setTargetListener(this);

      pool.start();

      started = true;
   }

   @Override
   public void stop() throws Exception {
      started = false;

      pool.setTargetListener(null);

      pool.stop();
   }

   public Target getTarget(Connection connection, String clientID, String username) {
      return getTarget(targetKeyResolver.resolve(connection, clientID, username));
   }

   public Target getTarget(String key) {

      if (this.localTargetFilter != null && this.localTargetFilter.matcher(key).matches()) {
         if (logger.isDebugEnabled()) {
            logger.debug("The " + targetKey + "[" + key + "] matches the localTargetFilter " + localTargetFilter.pattern());
         }

         return localTarget;
      }

      Target target = cache.getIfPresent(key);

      if (target != null && !pool.isTargetReady(target)) {
         if (logger.isDebugEnabled()) {
            logger.debug("The cache returns [" + target + "] not ready for " + targetKey + "[" + key + "]");
         }

         target = null;
      }

      if (target == null) {
         List<Target> targets = pool.getTargets();

         target = policy.selectTarget(targets, key);

         if (logger.isDebugEnabled()) {
            logger.debug("The policy selects [" + target + "] from " + targets + " for " + targetKey + "[" + key + "]");
         }

         if (target != null) {
            cache.put(key, target);
         }
      }

      return target;
   }

   @Override
   public void targetConnected(Target target) {

   }

   @Override
   public void targetSessionCreated(Target target, String id, String remoteAddress, String sniHost, String clientID, String username) {
      String key = targetKeyResolver.resolve(remoteAddress, sniHost, clientID, username);

      if (this.localTargetFilter != null && this.localTargetFilter.matcher(key).matches()) {
         if (logger.isDebugEnabled()) {
            logger.debug("Skip the " + targetKey + "[" + key + "] of session [" + id + "] because it matches the localTargetFilter " + localTargetFilter.pattern());
         }
      } else {
         if (logger.isDebugEnabled()) {
            logger.debug("Add the " + targetKey + "[" + key + "] of session [" + id + "] to the cache for the target " + target);
         }

         cache.put(key, target);
      }
   }

   @Override
   public void targetDisconnected(Target target) {

   }
}
