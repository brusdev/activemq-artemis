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

package org.apache.activemq.artemis.core.server.balancing.targets;

import org.apache.activemq.artemis.core.server.ActiveMQComponent;
import org.apache.activemq.artemis.core.server.balancing.pools.Pool;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class TargetController implements ActiveMQComponent, Runnable {
   private final Target target;

   private final Pool pool;

   private final ScheduledExecutorService scheduledExecutor;

   private boolean targetReady = false;

   private ScheduledFuture scheduledFuture;


   public Target getTarget() {
      return target;
   }

   public boolean isTargetReady() {
      return targetReady;
   }


   public TargetController(Target target, Pool pool, ScheduledExecutorService scheduledExecutor) {
      this.target = target;

      this.pool = pool;

      this.scheduledExecutor = scheduledExecutor;
   }

   @Override
   public void run() {
      try {
         if (!target.isConnected()) {
            target.connect();
         }

         target.checkReadiness();

         for (TargetTask targetTask : pool.getTargetTasks()) {
            targetTask.call(target);
         }
      } catch (Exception e) {
         e.printStackTrace();
      }
   }

   @Override
   public void start() throws Exception {
      scheduledFuture = scheduledExecutor.scheduleWithFixedDelay(this, 0, 5000, TimeUnit.MILLISECONDS);
   }

   @Override
   public void stop() throws Exception {
      if (scheduledFuture != null) {
         scheduledFuture.cancel(true);
      }
   }

   @Override
   public boolean isStarted() {
      return false;
   }
}