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

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQComponent;
import org.apache.activemq.artemis.core.server.balancing.caches.Cache;
import org.apache.activemq.artemis.core.server.balancing.policies.Policy;
import org.apache.activemq.artemis.core.server.balancing.pools.Pool;
import org.apache.activemq.artemis.core.server.balancing.targets.Target;
import org.apache.activemq.artemis.core.server.balancing.targets.TargetResult;
import org.apache.activemq.artemis.core.server.balancing.transformers.KeyTransformer;
import org.apache.activemq.artemis.spi.core.remoting.Connection;
import org.jboss.logging.Logger;

import java.util.List;
import java.util.regex.Pattern;

public class BrokerBalancer implements ActiveMQComponent {
   private static final Logger logger = Logger.getLogger(BrokerBalancer.class);

   public static final String CLIENT_ID_PREFIX = ActiveMQDefaultConfiguration.DEFAULT_INTERNAL_NAMING_PREFIX + "balancer.client.";

   private final String name;

   private final ConnectionKey connectionKey;

   private final ConnectionKeyResolver connectionKeyResolver;

   private final KeyTransformer connectionKeyTransformer;

   private final TargetResult localTarget;

   private volatile Pattern localTargetFilter;

   private final Pool pool;

   private final Policy policy;

   private final Cache cache;

   private volatile boolean started = false;

   public String getName() {
      return name;
   }

   public ConnectionKey getConnectionKey() {
      return connectionKey;
   }

   public ConnectionKeyResolver getConnectionKeyResolver() {
      return connectionKeyResolver;
   }

   public KeyTransformer getConnectionKeyTransformer() {
      return connectionKeyTransformer;
   }

   public Target getLocalTarget() {
      return localTarget.getTarget();
   }

   public String getLocalTargetFilter() {
      return localTargetFilter != null ? localTargetFilter.pattern() : null;
   }

   public void setLocalTargetFilter(String localTargetFilter) {
      if (localTargetFilter == null || localTargetFilter.trim().isEmpty()) {
         this.localTargetFilter = null;
      } else {
         this.localTargetFilter = Pattern.compile(localTargetFilter);
      }
   }

   public Pool getPool() {
      return pool;
   }

   public Policy getPolicy() {
      return policy;
   }

   public Cache getCache() {
      return cache;
   }

   @Override
   public boolean isStarted() {
      return started;
   }


   public BrokerBalancer(final String name,
                         final ConnectionKey connectionKey,
                         final String connectionKeyFilter,
                         final KeyTransformer connectionKeyTransformer,
                         final Target localTarget,
                         final String localTargetFilter,
                         final Cache cache,
                         final Pool pool,
                         final Policy policy) {
      this.name = name;

      this.connectionKey = connectionKey;

      this.connectionKeyResolver = new ConnectionKeyResolver(connectionKey, connectionKeyFilter);

      this.connectionKeyTransformer = connectionKeyTransformer;

      this.localTarget = new TargetResult(localTarget);

      this.localTargetFilter = localTargetFilter != null ? Pattern.compile(localTargetFilter) : null;

      this.pool = pool;

      this.policy = policy;

      this.cache = cache;
   }

   @Override
   public void start() throws Exception {
      if (pool != null) {
         pool.start();
      }

      started = true;
   }

   @Override
   public void stop() throws Exception {
      started = false;

      if (pool != null) {
         pool.stop();
      }
   }

   public TargetResult getTarget(Connection connection, String clientID, String username) {
      if (clientID != null && clientID.startsWith(BrokerBalancer.CLIENT_ID_PREFIX)) {
         if (logger.isDebugEnabled()) {
            logger.debug("The clientID [" + clientID + "] starts with BrokerBalancer.CLIENT_ID_PREFIX");
         }

         return localTarget;
      }

      return getTarget(connectionKeyResolver.resolve(connection, clientID, username));
   }

   public TargetResult getTarget(String key) {
      if (connectionKeyTransformer != null) {
         key = connectionKeyTransformer.transform(key);

         if (logger.isDebugEnabled()) {
            logger.debug("The key is transformed to " + key);
         }
      }

      if (this.localTargetFilter != null && this.localTargetFilter.matcher(key).matches()) {
         if (logger.isDebugEnabled()) {
            logger.debug("The " + connectionKey + "[" + key + "] matches the localTargetFilter " + localTargetFilter.pattern());
         }

         return localTarget;
      }

      if (pool == null) {
         return TargetResult.REFUSED_USE_ANOTHER_RESULT;
      }

      if (cache != null) {
         String nodeId = cache.get(key);

         logger.info("The cache returns target [" + nodeId + "] for " + connectionKey + "[" + key + "]");

         if (nodeId != null) {
            Target target = pool.getReadyTarget(nodeId);

            if (target != null) {
               if (logger.isDebugEnabled()) {
                  logger.info("The target [" + nodeId + "] is ready for " + connectionKey + "[" + key + "]");
               }

               return new TargetResult(target);
            }

            if (logger.isDebugEnabled()) {
               logger.info("The target [" + nodeId + "] is not ready for " + connectionKey + "[" + key + "]");
            }
         }
      }

      List<Target> targets = pool.getReadyTargets();

      Target target = policy.selectTarget(targets, key);

      if (logger.isDebugEnabled()) {
         logger.debug("The policy selects [" + target + "] from " + targets + " for " + connectionKey + "[" + key + "]");
      }

      if (target != null) {
         if (cache != null) {
            if (logger.isDebugEnabled()) {
               logger.debug("Caching " + connectionKey + "[" + key + "] for [" + target + "]");
            }
            cache.put(key, target.getNodeID());
         }

         return new TargetResult(target);
      }

      return TargetResult.REFUSED_UNAVAILABLE_RESULT;
   }
}