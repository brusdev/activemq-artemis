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

public class FirstElementBalancerPolicy extends BalancerPolicy {
   public static final String NAME = "FIRST_ELEMENT";

   private BalancerPool pool;

   public FirstElementBalancerPolicy() {
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
         return selectTargetsNext(Collections.singletonList(targets.get(0)), key);
      } else if (targets.size() > 0) {
         return selectTargetsNext(targets, key);
      }

      return targets;
   }
}
