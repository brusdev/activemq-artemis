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

package org.apache.activemq.artemis.core.server.balancer.policies;

import org.apache.activemq.artemis.core.server.balancer.BalancerController;
import org.apache.activemq.artemis.core.server.balancer.BalancerTarget;
import org.apache.activemq.artemis.core.server.balancer.pools.BalancerPool;
import org.apache.activemq.artemis.core.server.balancer.pools.BalancerPoolTask;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

public class LeastConnectionsBalancerPolicy extends BalancerPolicy {
   public static final String NAME = "LEAST_CONNECTIONS";

   public static final String GET_CONNECTION_COUNT_TASK_NAME = "GET_CONNECTION_COUNT_TASK";

   private BalancerPool pool;

   public LeastConnectionsBalancerPolicy() {
      super(NAME);
   }

   @Override
   public void load(BalancerController controller) {
      pool = controller.getPool();

      pool.addTask(new BalancerPoolTask("GET_CONNECTION_COUNT", balancerTarget -> {
         try {
            return balancerTarget.getAttribute(Integer.class, "broker", "ConnectionCount");
         } catch (Exception e) {
            e.printStackTrace();
         }
         return null;
      }));
   }

   @Override
   public void unload() {

   }

   @Override
   public List<BalancerTarget> selectTargets(List<BalancerTarget> targets, String key) {
      if (targets.size() > 1) {
         NavigableMap<Integer, List<BalancerTarget>> sortedTargets = new TreeMap<>();

         for (BalancerTarget target : targets) {
            Integer connectionCount = (Integer)target.getTaskResult(GET_CONNECTION_COUNT_TASK_NAME);

            if (connectionCount == null) {
               connectionCount = -1;
            }

            List<BalancerTarget> leastTargets = sortedTargets.get(connectionCount);

            if (leastTargets == null) {
               leastTargets = new ArrayList<>();
               sortedTargets.put(connectionCount, leastTargets);
            }

            leastTargets.add(target);
         }

         return sortedTargets.firstEntry().getValue();
      } else if (targets.size() > 0) {
         return selectTargetsNext(targets, key);
      }

      return targets;
   }
}
