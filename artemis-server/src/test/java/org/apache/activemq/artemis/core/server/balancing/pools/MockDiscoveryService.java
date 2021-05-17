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

import org.apache.activemq.artemis.api.core.TransportConfiguration;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiConsumer;

public class MockDiscoveryService extends DiscoveryService {
   private final Map<String, TransportConfiguration> entries = new HashMap<>();

   private final Map<String, TransportConfiguration> pendingEntries = new HashMap<>();

   private volatile boolean started;


   public Map<String, TransportConfiguration> getEntries() {
      return entries;
   }

   public Map<String, TransportConfiguration> getPendingEntries() {
      return pendingEntries;
   }

   @Override
   public boolean isStarted() {
      return started;
   }

   public void addEntry() {
      addEntry(UUID.randomUUID().toString(), new TransportConfiguration());
   }

   public void addEntry(String nodeID, TransportConfiguration connector) {
      if (started) {
         entries.put(nodeID, connector);
         fireEntryAddedEvent(nodeID, connector);
      } else {
         pendingEntries.put(nodeID, connector);
      }
   }

   public void removeEntry(String nodeID) {
      if (started) {
         TransportConfiguration removedConnector = entries.remove(nodeID);
         fireEntryRemovedEvent(nodeID, removedConnector);
      } else {
         pendingEntries.remove(nodeID);
      }
   }


   @Override
   public void start() throws Exception {
      started = true;

      pendingEntries.forEach(new BiConsumer<String, TransportConfiguration>() {
         @Override
         public void accept(String nodeID, TransportConfiguration connector) {
            entries.put(nodeID, connector);
            fireEntryAddedEvent(nodeID, connector);
         }
      });
   }

   @Override
   public void stop() throws Exception {
      started = false;
   }
}
