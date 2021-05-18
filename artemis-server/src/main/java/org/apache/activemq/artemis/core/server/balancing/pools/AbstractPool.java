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
import org.apache.activemq.artemis.core.server.balancing.targets.Target;
import org.apache.activemq.artemis.core.server.balancing.targets.TargetFactory;
import org.apache.activemq.artemis.core.server.balancing.targets.TargetListener;
import org.apache.activemq.artemis.core.server.balancing.targets.TargetReference;
import org.apache.activemq.artemis.core.server.balancing.targets.TargetTask;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public abstract class AbstractPool implements Pool {
   private static final Logger logger = Logger.getLogger(Pool.class);

   private final TargetFactory targetFactory;

   private final ScheduledExecutorService scheduledExecutor;

   private final int checkPeriod;

   private final List<TargetTask> targetTasks = new ArrayList<>();

   private final Map<String, TargetRunner> targetRunners = new ConcurrentHashMap<>();

   private String username;

   private String password;

   private volatile boolean started = false;


   @Override
   public String getUsername() {
      return username;
   }

   @Override
   public void setUsername(String username) {
      this.username = username;
   }

   @Override
   public String getPassword() {
      return password;
   }

   @Override
   public void setPassword(String password) {
      this.password = password;
   }

   @Override
   public int getCheckPeriod() {
      return checkPeriod;
   }

   @Override
   public List<Target> getAllTargets() {
      return targetRunners.values().stream().map(targetRunner -> targetRunner.getTarget()).collect(Collectors.toList());
   }

   @Override
   public List<Target> getTargets() {
      return targetRunners.values().stream().filter(targetRunner -> targetRunner.isTargetReady()).map(targetRunner -> targetRunner.getTarget()).collect(Collectors.toList());
   }

   @Override
   public List<TargetTask> getTargetTasks() {
      return targetTasks;
   }

   @Override
   public boolean isStarted() {
      return started;
   }


   public AbstractPool(TargetFactory targetFactory, ScheduledExecutorService scheduledExecutor, int checkPeriod) {
      this.targetFactory = targetFactory;

      this.scheduledExecutor = scheduledExecutor;

      this.checkPeriod = checkPeriod;
   }

   @Override
   public Target getTarget(String nodeId) {
      TargetRunner targetRunner = targetRunners.get(nodeId);

      return targetRunner != null ? targetRunner.getTarget() : null;
   }

   @Override
   public boolean isTargetReady(String nodeId) {
      TargetRunner targetRunner = targetRunners.get(nodeId);

      return targetRunner != null ? targetRunner.isTargetReady() : false;
   }

   @Override
   public void addTargetTask(TargetTask task) {
      targetTasks.add(task);
   }

   @Override
   public void removeTargetTask(TargetTask task) {
      targetTasks.remove(task);
   }

   @Override
   public void start() throws Exception {
      for (TargetRunner targetRunner : targetRunners.values()) {
         targetRunner.schedule();
      }

      started = true;
   }

   @Override
   public void stop() throws Exception {
      started = false;

      List<TargetRunner> targetRunners = new ArrayList<>(this.targetRunners.values());

      for (TargetRunner targetRunner : targetRunners) {
         removeTarget(targetRunner.getTarget().getReference().getNodeID());
      }
   }


   protected void addTarget(String nodeId, TransportConfiguration connector) throws Exception {
      Target target = targetFactory.createTarget(new TargetReference(nodeId, connector));
      TargetRunner targetRunner = new TargetRunner(target);

      targetRunners.put(nodeId, targetRunner);

      if (started) {
         targetRunner.schedule();
      }
   }

   protected Target removeTarget(String nodeId) throws Exception {
      TargetRunner targetRunner = targetRunners.remove(nodeId);

      if (targetRunner != null) {
         targetRunner.cancel();
      }

      return targetRunner != null ? targetRunner.getTarget() : null;
   }

   class TargetRunner implements Runnable, TargetListener {
      private final Target target;

      private ScheduledFuture scheduledFuture;

      private volatile boolean targetReady = false;

      public Target getTarget() {
         return target;
      }

      public boolean isTargetReady() {
         return targetReady;
      }


      TargetRunner(Target target) {
         this.target = target;

         this.target.setListener(this);
      }


      public void schedule() {
         scheduledFuture = scheduledExecutor.scheduleWithFixedDelay(
            this, 0, checkPeriod, TimeUnit.MILLISECONDS);
      }

      public void cancel() throws Exception {
         targetReady = false;

         target.setListener(null);

         if (scheduledFuture != null) {
            scheduledFuture.cancel(true);

            scheduledFuture = null;

            target.disconnect();
         }
      }

      @Override
      public void run() {
         try {
            if (scheduledFuture != null) {
               if (!target.isConnected()) {
                  target.connect();
               }

               target.checkReadiness();

               for (TargetTask targetTask : targetTasks) {
                  targetTask.call(target);
               }

               targetReady = true;
            }
         } catch (Exception e) {
            logger.debug("Target not ready", e);

            targetReady = false;
         }
      }

      @Override
      public void targetConnected() {

      }

      @Override
      public void targetDisconnected() {
         targetReady = false;
      }
   }
}
