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

package org.apache.activemq.artemis.core.server.balancing.policies;

import org.apache.activemq.artemis.core.server.balancing.targets.Target;
import org.apache.activemq.artemis.core.server.balancing.targets.TargetTask;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

public class LeastConnectionsPolicy extends Policy {
   public static final String NAME = "LEAST_CONNECTIONS";

   public static final String UPDATE_CONNECTION_COUNT_TASK_NAME = "UPDATE_CONNECTION_COUNT_TASK";

   private final Map<Target, Long> connectionCountCache = new HashMap<>();

   private final TargetTask[] targetTasks = new TargetTask[]{
      new TargetTask(UPDATE_CONNECTION_COUNT_TASK_NAME) {
         @Override
         public void call(Target target) {
            try {
               connectionCountCache.put(target, (Long)target.getAttribute("broker", "ConnectionCount", 3000));
            } catch (Exception e) {
               e.printStackTrace();
            }
         }
      }
   };

   @Override
   public TargetTask[] getTargetTasks() {
      return targetTasks;
   }

   public LeastConnectionsPolicy() {
      super(NAME);
   }

   @Override
   public List<Target> selectTargets(List<Target> targets, String key) {
      if (targets.size() > 1) {
         NavigableMap<Long, List<Target>> sortedTargets = new TreeMap<>();

         for (Target target : targets) {
            Long connectionCount = connectionCountCache.get(target);

            if (connectionCount == null) {
               connectionCount = Long.MAX_VALUE;
            }

            List<Target> leastTargets = sortedTargets.get(connectionCount);

            if (leastTargets == null) {
               leastTargets = new ArrayList<>();
               sortedTargets.put(connectionCount, leastTargets);
            }

            leastTargets.add(target);
         }

         return sortedTargets.firstEntry().getValue();
      } else if (targets.size() > 0) {
         return selectNextTargets(targets, key);
      }

      return targets;
   }
}
