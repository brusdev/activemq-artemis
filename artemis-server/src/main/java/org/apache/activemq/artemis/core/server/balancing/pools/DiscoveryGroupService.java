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

import org.apache.activemq.artemis.core.cluster.DiscoveryEntry;
import org.apache.activemq.artemis.core.cluster.DiscoveryGroup;
import org.apache.activemq.artemis.core.cluster.DiscoveryListener;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DiscoveryGroupService extends DiscoveryService implements DiscoveryListener {
   private final DiscoveryGroup discoveryGroup;

   private final Map<String, DiscoveryEntry> entries = new ConcurrentHashMap<>();

   public DiscoveryGroupService(DiscoveryGroup discoveryGroup) {
      this.discoveryGroup = discoveryGroup;
   }

   @Override
   public void start() throws Exception {
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
   public void connectorsChanged(List<DiscoveryEntry> newEntries) {
      Map<String, DiscoveryEntry> oldEntries = new HashMap<>(entries);

      for (DiscoveryEntry newEntry : newEntries) {
         DiscoveryEntry oldEntry = oldEntries.remove(newEntry.getNodeID());

         entries.put(newEntry.getNodeID(), newEntry);

         if (oldEntry == null) {
            fireEntryAddedEvent(newEntry);
         } else {
            fireEntryUpdatedEvent(oldEntry, newEntry);
         }
      }

      oldEntries.forEach((nodeID, entry) -> {
         entries.remove(nodeID);

         fireEntryRemovedEvent(entry);
      });
   }
}
