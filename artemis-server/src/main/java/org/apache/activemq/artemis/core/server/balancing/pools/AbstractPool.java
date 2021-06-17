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
import org.apache.activemq.artemis.core.server.balancing.targets.TargetMonitor;
import org.apache.activemq.artemis.core.server.balancing.targets.TargetProbe;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

public abstract class AbstractPool implements Pool {
   private final TargetFactory targetFactory;

   private final ScheduledExecutorService scheduledExecutor;

   private final int checkPeriod;

   private final List<TargetProbe> targetProbes = new ArrayList<>();

   private final Map<Target, TargetMonitor> targets = new ConcurrentHashMap<>();

   private final List<TargetMonitor> targetMonitors = new CopyOnWriteArrayList<>();

   private String username;

   private String password;

   private int quorumSize;

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
   public int getQuorumSize() {
      return quorumSize;
   }

   @Override
   public void setQuorumSize(int quorumSize) {
      this.quorumSize = quorumSize;
   }

   @Override
   public List<Target> getAllTargets() {
      return targetMonitors.stream().map(targetMonitor -> targetMonitor.getTarget()).collect(Collectors.toList());
   }

   @Override
   public List<Target> getTargets() {
      List<Target> targets = targetMonitors.stream().filter(targetMonitor -> targetMonitor.isTargetReady())
         .map(targetMonitor -> targetMonitor.getTarget()).collect(Collectors.toList());

      return targets.size() >= quorumSize ? targets : Collections.emptyList();
   }

   @Override
   public List<TargetProbe> getTargetProbes() {
      return targetProbes;
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
      for (TargetMonitor targetMonitor : targetMonitors) {
         if (nodeId.equals(targetMonitor.getTarget().getNodeID())) {
            return targetMonitor.getTarget();
         }
      }

      return null;
   }

   @Override
   public boolean isTargetReady(Target target) {
      TargetMonitor targetMonitor = targets.get(target);

      return targetMonitor != null ? targetMonitor.isTargetReady() : false;
   }

   @Override
   public void addTargetProbe(TargetProbe probe) {
      targetProbes.add(probe);
   }

   @Override
   public void removeTargetProbe(TargetProbe probe) {
      targetProbes.remove(probe);
   }

   @Override
   public void start() throws Exception {
      started = true;

      for (TargetMonitor targetMonitor : targetMonitors) {
         targetMonitor.start();
      }
   }

   @Override
   public void stop() throws Exception {
      started = false;

      List<TargetMonitor> targetMonitors = new ArrayList<>(this.targetMonitors);

      for (TargetMonitor targetMonitor : targetMonitors) {
         removeTarget(targetMonitor.getTarget());
      }
   }

   protected void addTarget(TransportConfiguration connector, String nodeID) throws Exception {
      addTarget(targetFactory.createTarget(connector, nodeID));
   }

   @Override
   public boolean addTarget(Target target) {
      if (targets.containsKey(target)) {
         return false;
      }

      TargetMonitor targetMonitor = new TargetMonitor(scheduledExecutor, checkPeriod, target, targetProbes);

      targets.put(target, targetMonitor);

      targetMonitors.add(targetMonitor);

      if (started) {
         targetMonitor.start();
      }

      return true;
   }

   @Override
   public boolean removeTarget(Target target) {
      TargetMonitor targetMonitor = targets.remove(target);

      if (targetMonitor == null) {
         return false;
      }

      targetMonitors.remove(targetMonitor);

      targetMonitor.stop();

      return true;
   }
}
