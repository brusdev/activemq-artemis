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

package org.apache.activemq.artemis.core.server.redirect;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.activemq.artemis.core.config.RedirectConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQComponent;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.redirect.policies.RedirectPolicy;
import org.apache.activemq.artemis.core.server.redirect.policies.RedirectPolicyFactory;
import org.apache.activemq.artemis.core.server.redirect.pools.DiscoveryRedirectPool;
import org.apache.activemq.artemis.core.server.redirect.pools.RedirectPool;
import org.apache.activemq.artemis.core.server.redirect.pools.StaticRedirectPool;
import org.apache.activemq.artemis.spi.core.security.jaas.RolePrincipal;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class RedirectController implements ActiveMQComponent {

   private final ActiveMQServer server;
   private final RedirectConfiguration config;
   private final RedirectPolicy policy;
   private final RedirectKeyType key;
   private final Cache<String, RedirectTarget> cache;
   private final RedirectPool pool;

   public RedirectPool getPool() {
      return pool;
   }

   public RedirectController(final ActiveMQServer server, final RedirectConfiguration config) {
      this.server = server;
      this.config = config;

      key = config.getKey();
      policy = RedirectPolicyFactory.getPolicy(config.getPolicy());

      cache = CacheBuilder.newBuilder().expireAfterAccess(3000, TimeUnit.MILLISECONDS).build();

      if (config.getDiscoveryGroupName() != null) {
         pool = new DiscoveryRedirectPool(server, config.getDiscoveryGroupName());
      } else {
         pool = new StaticRedirectPool(server, config.getStaticConnectors());
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

   public boolean match(RedirectingConnection connection) {
      return (config.getSourceIP() == null || config.getSourceIP().equals(connection.getSourceIP())) &&
            (config.getUser() == null || config.getUser().equals(connection.getUser())) &&
            (config.getUserRole() == null || new RolePrincipal(config.getUserRole()).implies(connection.getSubject()));
   }

   public RedirectTarget getTarget(RedirectingConnection connection) {
      try {
         return cache.get(connection.getValue(key), new Callable<RedirectTarget>() {
            @Override
            public RedirectTarget call() throws Exception {
               return policy.selectTarget(connection);
            }
         });
      } catch (ExecutionException e) {
         e.printStackTrace();
      }

      return null;
   }
}
