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

package org.apache.activemq.artemis.core.server.balancing.pools;

import org.apache.activemq.artemis.api.core.DiscoveryGroupConfiguration;
import org.apache.activemq.artemis.core.cluster.DiscoveryEntry;
import org.apache.activemq.artemis.core.cluster.DiscoveryGroup;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.balancing.BrokerBalancerTarget;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

public class DiscoveryPool extends Pool {
   private final String discoveryGroupName;

   private DiscoveryGroup discoveryGroup;

   public DiscoveryPool(ActiveMQServer server, ScheduledExecutorService scheduledExecutor, String discoveryGroupName) {
      super(server, scheduledExecutor);
      this.discoveryGroupName = discoveryGroupName;
   }

   @Override
   public void start() throws Exception {
      super.start();

      DiscoveryGroupConfiguration discoveryGroupConfiguration = getServer().getConfiguration().getDiscoveryGroupConfigurations().get(discoveryGroupName);
      discoveryGroup = new DiscoveryGroup(getServer().getNodeID().toString(), discoveryGroupName, discoveryGroupConfiguration.getRefreshTimeout(), discoveryGroupConfiguration.getBroadcastEndpointFactory(), null);
      discoveryGroup.registerListener(newConnectors -> {
         List<BrokerBalancerTarget> addingTargets = new ArrayList<>();
         Map<String, BrokerBalancerTarget> removingTragets = new HashMap<>();
         for (BrokerBalancerTarget target : getTargets()) {
            removingTragets.put(target.getNodeID(), target);
         }

         for (DiscoveryEntry newConnector : newConnectors) {
            BrokerBalancerTarget addingTarget = removingTragets.remove(newConnector.getNodeID());

            if (addingTarget == null) {
               addingTargets.add(new BrokerBalancerTarget(newConnector.getNodeID(), newConnector.getConnector()));
            }
         }

         for (BrokerBalancerTarget removingTraget : removingTragets.values()) {
            //removeTarget(removingTraget.getNodeID());
         }

         for (BrokerBalancerTarget addingTarget : addingTargets) {
            addTarget(addingTarget);
         }
      });
      discoveryGroup.start();
   }

   @Override
   public void stop() throws Exception {
      super.stop();

      if (discoveryGroup != null) {
         discoveryGroup.stop();
      }
   }

   @Override
   public boolean isStarted() {
      return false;
   }
}
