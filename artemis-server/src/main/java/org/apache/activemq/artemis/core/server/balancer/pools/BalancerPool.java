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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
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
      target.setState(BalancerTarget.State.ATTACHED);
   }

   public BalancerTarget getTarget(String nodeId) {
      return targets.get(nodeId);
   }

   public BalancerTarget removeTarget(String nodeId) {
      BalancerTarget target = targets.remove(nodeId);
      if (target != null) {
         target.setState(BalancerTarget.State.DETACHED);
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
            List<BalancerTarget> connectingTargets = new ArrayList<>(targets.values());

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
      }, 0, 0, TimeUnit.MILLISECONDS);


      checkScheduledFuture = scheduledExecutor.scheduleWithFixedDelay(new Runnable() {
         @Override
         public void run() {
            List<BalancerTarget> checkingTargets = new ArrayList<>(targets.values());

            for (BalancerTarget checkingTarget : checkingTargets) {
               try {
                  checkingTarget.getAttribute(int.class, "broker", "ConnectionCount");
               } catch (Exception e) {
                  e.printStackTrace();
               }


            }
         }
      }, 0, 0, TimeUnit.MILLISECONDS);

      taskScheduledFuture = scheduledExecutor.scheduleWithFixedDelay(new Runnable() {
         @Override
         public void run() {
            List<BalancerTarget> schedulingTargets = new ArrayList<>(targets.values());

            for (BalancerTarget schedulingTarget : schedulingTargets) {
             for (BalancerPoolTask task : tasks.values()) {
                  schedulingTarget.setTaskResult(task.getName(),
                     task.getTask().apply(schedulingTarget));
               }
            }
         }
      }, 0, 0, TimeUnit.MILLISECONDS);
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
