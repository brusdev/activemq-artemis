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
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.balancing.targets.CoreTarget;
import org.apache.activemq.artemis.core.server.balancing.targets.Target;
import org.apache.activemq.artemis.core.server.balancing.targets.TargetController;
import org.apache.activemq.artemis.core.server.balancing.targets.TargetReference;
import org.apache.activemq.artemis.core.server.balancing.targets.TargetTask;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

public abstract class AbstractPool implements Pool {
   private final ActiveMQServer server;
   private final ScheduledExecutorService scheduledExecutor;
   private final List<TargetTask> targetTasks = new ArrayList<>();
   private final Map<String, TargetController> targetControllers = new HashMap<>();

   private volatile boolean started = false;

   public ActiveMQServer getServer() {
      return server;
   }

   public ScheduledExecutorService getScheduledExecutor() {
      return scheduledExecutor;
   }

   @Override
   public List<Target> getAllTargets() {
      return targetControllers.values().stream().map(targetController -> targetController.getTarget()).collect(Collectors.toList());
   }

   @Override
   public List<Target> getTargets() {
      return targetControllers.values().stream().filter(targetController -> targetController.isStarted()).map(targetController -> targetController.getTarget()).collect(Collectors.toList());
   }

   @Override
   public List<TargetTask> getTargetTasks() {
      return targetTasks;
   }

   public AbstractPool(ActiveMQServer server, ScheduledExecutorService scheduledExecutor) {
      this.server = server;
      this.scheduledExecutor = scheduledExecutor;
   }

   @Override
   public void addTarget(String nodeId, TransportConfiguration connector) throws Exception {
      Target target = new CoreTarget(new TargetReference(nodeId, connector));
      TargetController targetController = new TargetController(target, this, scheduledExecutor);

      targetControllers.put(nodeId, targetController);

      if (started) {
         targetController.start();
      }
   }

   @Override
   public boolean isTargetReady(String nodeId) {
      TargetController targetController = targetControllers.get(nodeId);

      return targetController != null ? targetController.isTargetReady() : false;
   }

   @Override
   public Target getTarget(String nodeId) {
      TargetController targetController = targetControllers.get(nodeId);
      return targetController != null ? targetController.getTarget() : null;
   }

   @Override
   public Target removeTarget(String nodeId) throws Exception {
      TargetController targetController = targetControllers.remove(nodeId);

      if (targetController != null) {
         targetController.stop();
      }

      return targetController != null ? targetController.getTarget() : null;
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

   @Override
   public boolean isStarted() {
      return started;
   }
}
