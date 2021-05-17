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
import org.apache.activemq.artemis.core.server.ActiveMQComponent;

public abstract class DiscoveryService implements ActiveMQComponent {

   private Listener listener;

   public Listener getListener() {
      return listener;
   }

   public void setListener(Listener listener) {
      this.listener = listener;
   }

   protected void fireEntryAddedEvent(String nodeID, TransportConfiguration connector) {
      if (listener != null) {
         this.listener.entryAdded(nodeID, connector);
      }
   }

   protected void fireEntryRemovedEvent(String nodeID, TransportConfiguration connector) {
      if (listener != null) {
         this.listener.entryRemoved(nodeID, connector);
      }
   }

   public interface Listener {
      void entryAdded(String nodeID, TransportConfiguration connector);

      void entryRemoved(String nodeID, TransportConfiguration connector);
   }
}
