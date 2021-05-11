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

package org.apache.activemq.artemis.core.server.balancer.pools;

import org.apache.activemq.artemis.core.server.ActiveMQComponent;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.balancer.BalancerTarget;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public abstract class BalancerPool implements ActiveMQComponent {
   private final ActiveMQServer server;
   private final ScheduledExecutorService scheduledExecutor;
   private final Map<String, BalancerPoolTask> tasks = new HashMap<>();
   private final Map<String, BalancerTarget> targets = new HashMap<>();

   private ScheduledFuture checkScheduledFuture;
   private ScheduledFuture connectScheduledFuture;
   private ScheduledFuture taskScheduledFuture;

   public ActiveMQServer getServer() {
      return server;
   }

   public ScheduledExecutorService getScheduledExecutor() {
      return scheduledExecutor;
   }

   public List<BalancerTarget> getTargets() {
      return new ArrayList<>(targets.values());
   }

   public List<BalancerTarget> getTargets(BalancerTarget.State state) {
      return targets.values().stream().filter(balancerTarget ->
         balancerTarget.getState() == state).collect(Collectors.toList());
   }

   public BalancerPool(ActiveMQServer server, ScheduledExecutorService scheduledExecutor) {
      this.server = server;
      this.scheduledExecutor = scheduledExecutor;
   }

   public void addTarget(BalancerTarget target) {
      targets.put(target.getNodeID(), target);
      target.attach();
   }

   public BalancerTarget getTarget(String nodeId) {
      return targets.get(nodeId);
   }

   public BalancerTarget removeTarget(String nodeId) {
      BalancerTarget target = targets.remove(nodeId);
      if (target != null) {
         target.detach();
      }
      return target;
   }

   public void addTask(BalancerPoolTask task) {
      tasks.put(task.getName(), task);
   }

   public BalancerPoolTask getTask(String name) {
      return tasks.get(name);
   }

   public BalancerPoolTask removeTask(String name) {
      return tasks.remove(name);
   }

   @Override
   public void start() throws Exception {
      connectScheduledFuture = scheduledExecutor.scheduleWithFixedDelay(new Runnable() {
         @Override
         public void run() {
            List<BalancerTarget> connectingTargets = getTargets(BalancerTarget.State.ATTACHED);

            for (BalancerTarget connectingTarget : connectingTargets) {
               if (connectingTarget.getState() == BalancerTarget.State.ATTACHED) {
                  try {
                     connectingTarget.connect();
                  } catch (Exception e) {
                     e.printStackTrace();
                  }
               }
            }
         }
      }, 0, 5000, TimeUnit.MILLISECONDS);

      checkScheduledFuture = scheduledExecutor.scheduleWithFixedDelay(new Runnable() {
         @Override
         public void run() {
            List<BalancerTarget> checkingTargets = getTargets(BalancerTarget.State.CONNECTED);

            for (BalancerTarget checkingTarget : checkingTargets) {
               if (checkingTarget.getState() == BalancerTarget.State.CONNECTED) {
                  try {
                     Boolean activated = checkingTarget.getAttribute(Boolean.class, "broker", "Active");

                     if (activated) {
                        checkingTarget.active();
                     }
                  } catch (Exception e) {
                     e.printStackTrace();
                  }
               }
            }
         }
      }, 6000, 5000, TimeUnit.MILLISECONDS);

      taskScheduledFuture = scheduledExecutor.scheduleWithFixedDelay(new Runnable() {
         @Override
         public void run() {
            if (tasks.size() > 0) {
               List<BalancerTarget> schedulingTargets = getTargets(BalancerTarget.State.ACTIVATED);

               for (BalancerTarget schedulingTarget : schedulingTargets) {
                  if (schedulingTarget.getState() == BalancerTarget.State.ACTIVATED) {
                     for (BalancerPoolTask task : tasks.values()) {
                        schedulingTarget.setTaskResult(task.getName(),
                           task.getTask().apply(schedulingTarget));
                     }
                  }
               }
            }
         }
      }, 9000, 5000, TimeUnit.MILLISECONDS);
   }

   @Override
   public void stop() throws Exception {
      if (checkScheduledFuture != null) {
         checkScheduledFuture.cancel(true);
      }
   }

   @Override
   public boolean isStarted() {
      return false;
   }
}
