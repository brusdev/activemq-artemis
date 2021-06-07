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

import org.apache.activemq.artemis.core.server.balancing.targets.TargetFactory;
import org.jboss.logging.Logger;

import java.util.concurrent.ScheduledExecutorService;

public class DiscoveryPool extends AbstractPool implements DiscoveryService.Listener {
   private static final Logger logger = Logger.getLogger(DiscoveryPool.class);

   private final DiscoveryService discoveryService;

   private boolean autoRemove = false;

   public boolean isAutoRemove() {
      return autoRemove;
   }

   public void setAutoRemove(boolean autoRemove) {
      this.autoRemove = autoRemove;
   }

   public DiscoveryPool(TargetFactory targetFactory, ScheduledExecutorService scheduledExecutor,
                        int checkPeriod, DiscoveryService discoveryService) {
      super(targetFactory, scheduledExecutor, checkPeriod);

      this.discoveryService = discoveryService;
   }

   @Override
   public void start() throws Exception {
      super.start();

      discoveryService.setListener(this);

      discoveryService.start();
   }

   @Override
   public void stop() throws Exception {
      super.stop();

      if (discoveryService != null) {
         discoveryService.setListener(null);

         discoveryService.stop();
      }
   }

   @Override
   public void entryAdded(DiscoveryService.Entry entry) {
      try {
         addTarget(entry.getNodeID(), entry.getConnector());
      } catch (Exception e) {
         logger.debug("Error on adding the target for " + entry);
      }
   }

   @Override
   public void entryRemoved(DiscoveryService.Entry entry) {
      if (autoRemove) {
         try {
            removeTarget(entry.getNodeID());
         } catch (Exception e) {
            logger.debug("Error on removing the target for " + entry);
         }
      }
   }

   @Override
   public void entryUpdated(DiscoveryService.Entry oldEntry, DiscoveryService.Entry newEntry) {
      try {
         removeTarget(oldEntry.getNodeID());

         addTarget(newEntry.getNodeID(), newEntry.getConnector());
      } catch (Exception e) {
         logger.debug("Error on updating the target for " + newEntry);
      }
   }
}
