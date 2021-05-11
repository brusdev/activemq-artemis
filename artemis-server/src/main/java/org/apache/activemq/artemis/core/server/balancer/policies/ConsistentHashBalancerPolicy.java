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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

public class ConsistentHashBalancerPolicy extends BalancerPolicy {
   public static final String NAME = "CONSISTENT_HASH";

   private BalancerPool pool;

   public ConsistentHashBalancerPolicy() {
      super(NAME);
   }

   @Override
   public void load(BalancerController controller) {
      pool = controller.getPool();
   }

   @Override
   public void unload() {

   }

   @Override
   public List<BalancerTarget> selectTargets(List<BalancerTarget> targets, String key) {
      if (targets.size() > 1) {
         NavigableMap<Integer, BalancerTarget> consistentTargets = new TreeMap<>();

         for (BalancerTarget target : pool.getTargets()) {
            consistentTargets.put(target.getNodeID().hashCode(), target);
         }

         if (consistentTargets.size() > 0) {
            Map.Entry<Integer, BalancerTarget> consistentEntry = consistentTargets.floorEntry(key.hashCode());

            if (consistentEntry == null) {
               consistentEntry = consistentTargets.firstEntry();
            }

            return selectTargetsNext(Collections.singletonList(consistentEntry.getValue()), key);
         }
      } else if (targets.size() > 0) {
         return selectTargetsNext(targets, key);
      }

      return targets;
   }
}
