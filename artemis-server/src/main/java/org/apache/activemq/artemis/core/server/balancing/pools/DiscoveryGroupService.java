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

import org.apache.activemq.artemis.api.core.BroadcastEndpointFactory;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.cluster.DiscoveryEntry;
import org.apache.activemq.artemis.core.cluster.DiscoveryGroup;
import org.apache.activemq.artemis.core.cluster.DiscoveryListener;
import org.apache.activemq.artemis.core.server.management.NotificationService;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DiscoveryGroupService extends DiscoveryService implements DiscoveryListener {
   private final String nodeID;
   private final String name;
   private final long timeout;
   private final BroadcastEndpointFactory endpointFactory;
   private final NotificationService service;

   private DiscoveryGroup discoveryGroup;
   private Map<String, TransportConfiguration> entries;

   public DiscoveryGroupService(final String nodeID,
                                final String name,
                                final long timeout,
                                BroadcastEndpointFactory endpointFactory,
                                NotificationService service) {
      this.nodeID = nodeID;
      this.name = name;
      this.timeout = timeout;
      this.endpointFactory = endpointFactory;
      this.service = service;
   }

   @Override
   public void start() throws Exception {
      entries = new HashMap<>();

      discoveryGroup = new DiscoveryGroup(nodeID, name, timeout, endpointFactory, service);

      discoveryGroup.start();
   }

   @Override
   public void stop() throws Exception {
      discoveryGroup.stop();

      entries.clear();
   }

   @Override
   public boolean isStarted() {
      return discoveryGroup.isStarted();
   }

   @Override
   public void connectorsChanged(List<DiscoveryEntry> newConnectors) {
      Map<String, TransportConfiguration> addingEntries = new HashMap<>();
      Map<String, TransportConfiguration> removingEntries = new HashMap<>(entries);

      for (DiscoveryEntry newConnector : newConnectors) {
         TransportConfiguration removedConnector = removingEntries.remove(newConnector.getNodeID());

         if (removedConnector == null) {
            addingEntries.put(newConnector.getNodeID(), newConnector.getConnector());
         }
      }

      addingEntries.forEach((nodeID, connector) -> fireEntryAddedEvent(nodeID, connector));

      removingEntries.forEach((nodeID, connector) -> fireEntryRemovedEvent(nodeID, connector));
   }
}
