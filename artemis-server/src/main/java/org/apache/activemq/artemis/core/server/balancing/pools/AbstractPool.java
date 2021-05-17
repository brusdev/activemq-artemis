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
import org.apache.activemq.artemis.core.server.balancing.targets.TargetController;
import org.apache.activemq.artemis.core.server.balancing.targets.TargetFactory;
import org.apache.activemq.artemis.core.server.balancing.targets.TargetReference;
import org.apache.activemq.artemis.core.server.balancing.targets.TargetTask;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

public abstract class AbstractPool implements Pool {
   private final TargetFactory targetFactory;

   private final ScheduledExecutorService scheduledExecutor;

   private final int checkPeriod;

   private final List<TargetTask> targetTasks = new ArrayList<>();

   private final Map<String, TargetController> targetControllers = new ConcurrentHashMap<>();

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
      return targetControllers.values().stream().map(targetController -> targetController.getTarget()).collect(Collectors.toList());
   }

   @Override
   public List<Target> getTargets() {
      return targetControllers.values().stream().filter(targetController -> targetController.isTargetReady()).map(targetController -> targetController.getTarget()).collect(Collectors.toList());
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
   public boolean checkTarget(String nodeId) {
      TargetController targetController = targetControllers.get(nodeId);

      return targetController != null ? targetController.isTargetReady() : false;
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
      for (TargetController targetController : targetControllers.values()) {
         targetController.start();
      }

      started = true;
   }

   @Override
   public void stop() throws Exception {
      started = false;

      List<TargetController> targetControllers = new ArrayList<>(this.targetControllers.values());

      for (TargetController targetController : targetControllers) {
         removeTarget(targetController.getTarget().getReference().getNodeID());
      }
   }


   protected void addTarget(String nodeId, TransportConfiguration connector) throws Exception {
      Target target = targetFactory.createTarget(new TargetReference(nodeId, connector));
      TargetController targetController = new TargetController(target, targetTasks, scheduledExecutor, checkPeriod);

      targetControllers.put(nodeId, targetController);

      if (started) {
         targetController.start();
      }
   }

   protected Target removeTarget(String nodeId) throws Exception {
      TargetController targetController = targetControllers.remove(nodeId);

      if (targetController != null) {
         targetController.stop();
      }

      return targetController != null ? targetController.getTarget() : null;
   }
}
