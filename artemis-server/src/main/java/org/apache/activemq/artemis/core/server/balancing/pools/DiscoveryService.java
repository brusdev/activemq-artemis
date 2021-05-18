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
import org.apache.activemq.artemis.core.server.ActiveMQComponent;

public abstract class DiscoveryService implements ActiveMQComponent {

   private Listener listener;

   public Listener getListener() {
      return listener;
   }

   public void setListener(Listener listener) {
      this.listener = listener;
   }

   protected void fireEntryAddedEvent(DiscoveryEntry entry) {
      if (listener != null) {
         this.listener.entryAdded(entry);
      }
   }

   protected void fireEntryRemovedEvent(DiscoveryEntry entry) {
      if (listener != null) {
         this.listener.entryRemoved(entry);
      }
   }

   protected void fireEntryUpdatedEvent(DiscoveryEntry oldEntry, DiscoveryEntry newEntry) {
      if (listener != null) {
         this.listener.entryUpdated(oldEntry, newEntry);
      }
   }


   public interface Listener {
      void entryAdded(DiscoveryEntry entry);

      void entryRemoved(DiscoveryEntry entry);

      void entryUpdated(DiscoveryEntry oldEntry, DiscoveryEntry newEntry);
   }
}
