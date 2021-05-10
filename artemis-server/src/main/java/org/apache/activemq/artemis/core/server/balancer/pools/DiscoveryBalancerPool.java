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

import org.apache.activemq.artemis.api.core.DiscoveryGroupConfiguration;
import org.apache.activemq.artemis.core.cluster.DiscoveryEntry;
import org.apache.activemq.artemis.core.cluster.DiscoveryGroup;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.balancer.BalancerTarget;

import java.util.ArrayList;
import java.util.List;

public class DiscoveryBalancerPool implements BalancerPool {
   private final ActiveMQServer server;
   private final String discoveryGroupName;
   private final ArrayList<BalancerTarget> targets = new ArrayList<>();

   private DiscoveryGroup discoveryGroup;

   public DiscoveryBalancerPool(ActiveMQServer server, String discoveryGroupName) {
      this.server = server;
      this.discoveryGroupName = discoveryGroupName;
   }

   @Override
   public void start() throws Exception {
      DiscoveryGroupConfiguration discoveryGroupConfiguration = server.getConfiguration().getDiscoveryGroupConfigurations().get(discoveryGroupName);
      discoveryGroup = new DiscoveryGroup(server.getNodeID().toString(), discoveryGroupName, discoveryGroupConfiguration.getRefreshTimeout(), discoveryGroupConfiguration.getBroadcastEndpointFactory(), null);
      discoveryGroup.registerListener(newConnectors -> {
         targets.clear();
         for (DiscoveryEntry newConnector : newConnectors) {
            targets.add(new BalancerTarget(newConnector.getNodeID(), newConnector.getConnector()));
         }
      });
      discoveryGroup.start();
   }

   @Override
   public void stop() throws Exception {
      if (discoveryGroup != null) {
         discoveryGroup.stop();
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
