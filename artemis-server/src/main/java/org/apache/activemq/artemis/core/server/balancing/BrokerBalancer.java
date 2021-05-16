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
import org.apache.activemq.artemis.spi.core.remoting.Connection;
import org.jboss.logging.Logger;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BrokerBalancer implements ActiveMQComponent {
   private static final Logger logger = Logger.getLogger(BrokerBalancer.class);

   private static final Pattern ipv4Pattern = Pattern.compile("^(([0-9]|[1-9][0-9]|1[0-9][0-9]|2[0-4][0-9]|25[0-5])(\\.(?!$)|$)){4}$");


   private final String name;

   private final TargetKey targetKey;

   private final Pattern localFilter;

   private final Target localTarget;

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

   public String getLocalFilter() {
      return localFilter != null ? localFilter.pattern() : null;
   }

   public Target getLocalTarget() {
      return localTarget;
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


   public BrokerBalancer(final String name, final TargetKey targetKey, final String localFilter, final Target localTarget, final Pool pool, final Policy policy, final int affinityTimeout) {
      this.name = name;

      this.targetKey = targetKey;

      this.localFilter = localFilter != null ? Pattern.compile(localFilter) : null;

      this.localTarget = localTarget;

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

   public Target getTarget(Connection connection, String username) {
      String key = null;

      switch (targetKey) {
         case SNI_HOST:
            key = connection.getSNIHostName();
            break;
         case SOURCE_IP:
            if (connection.getRemoteAddress() != null) {
               Matcher ipv4Matcher = ipv4Pattern.matcher(connection.getRemoteAddress());

               if (ipv4Matcher.find()) {
                  key = ipv4Matcher.group();
               }
            }
            break;
         case USER_NAME:
            key = username;
            break;
         default:
            throw new IllegalStateException("Unexpected value: " + targetKey);
      }

      if (key == null) {
         key = "DEFAULT";
      }

      return getTarget(key);
   }

   public Target getTarget(String key) {

      if (this.localFilter != null && this.localFilter.matcher(key).matches()) {
         return localTarget;
      }

      Target target = cache.getIfPresent(key);

      if (target != null && !pool.isTargetReady(target)) {
         if (logger.isDebugEnabled()) {
            logger.debug("The cache returns [" + target + "] not ready for " + key);
         }

         target = null;
      }

      if (target == null) {
         List<Target> targets = pool.getTargets();

         target = policy.selectTarget(targets, key);

         if (logger.isDebugEnabled()) {
            logger.debug("The policy selects [" + target + "] from " + targets + " for " + key);
         }

         if (target != null) {
            cache.put(key, target);
         }
      }

      return target;
   }
}
