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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.BalancerConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQComponent;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.jboss.logging.Logger;

public class BalancerManager implements ActiveMQComponent {
   private static final Logger logger = Logger.getLogger(BalancerManager.class);

   private final Configuration config;
   private final ActiveMQServer server;
   private final ScheduledExecutorService scheduledExecutor;

   private Map<String, BalancerController> balancerControllers = new HashMap<>();


   public BalancerManager(final Configuration config, final ActiveMQServer server, ScheduledExecutorService scheduledExecutor) {
      this.config = config;
      this.server = server;
      this.scheduledExecutor = scheduledExecutor;
   }

   public void deploy() {
      for (BalancerConfiguration balancerConfig : config.getBalancerConfigurations()) {
         balancerControllers.put(balancerConfig.getName(), new BalancerController(balancerConfig, server, scheduledExecutor));
      }
   }

   @Override
   public void start() throws Exception {
      for (BalancerController balancerController : balancerControllers.values()) {
         balancerController.start();
      }
   }

   @Override
   public void stop() throws Exception {
      for (BalancerController balancerController : balancerControllers.values()) {
         balancerController.stop();
      }
   }

   @Override
   public boolean isStarted() {
      return false;
   }

   public BalancerController getBalancer(String name) {
      return balancerControllers.get(name);
   }
}
