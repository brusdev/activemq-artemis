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

package org.apache.activemq.artemis.core.server.balancer.pools;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.balancer.BalancerTarget;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class StaticBalancerPool implements BalancerPool {
   private final List<String> staticConnectors;
   private final ScheduledExecutorService scheduledExecutor;
   private final ArrayList<BalancerTarget> targets = new ArrayList<>();

   private ScheduledFuture checkScheduledFuture;

   public StaticBalancerPool(List<String> staticConnectors, ActiveMQServer server, ScheduledExecutorService scheduledExecutor) {
      this.staticConnectors = staticConnectors;
      this.scheduledExecutor = scheduledExecutor;

      Map<String, TransportConfiguration> connectorConfigurations =
            server.getConfiguration().getConnectorConfigurations();

      for (String connector : staticConnectors) {
         targets.add(new BalancerTarget(connector, connectorConfigurations.get(connector)));
      }
   }

   @Override
   public void start() throws Exception {
      checkScheduledFuture = this.scheduledExecutor.scheduleWithFixedDelay(new Runnable() {
         @Override
         public void run() {






         }
      }, 0, 0, TimeUnit.MILLISECONDS);

   }

   @Override
   public void stop() throws Exception {
      if (checkScheduledFuture != null) {
         checkScheduledFuture.cancel(true);
      }
   }

   @Override
   public boolean isStarted() {
      return false;
   }

   @Override
   public List<BalancerTarget> getTargets() {
      return targets;
   }
}
