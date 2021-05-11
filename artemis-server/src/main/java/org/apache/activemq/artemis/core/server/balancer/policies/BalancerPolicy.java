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

import java.util.List;

public abstract class BalancerPolicy {
   private String name;

   private BalancerPolicy next;

   public String getName() {
      return name;
   }

   public BalancerPolicy getNext() {
      return next;
   }

   public BalancerPolicy setNext(BalancerPolicy next) {
      this.next = next;
      return this;
   }

   public BalancerPolicy(String name) {

   }

   public abstract void load(BalancerController controller);

   public abstract void unload();

   public abstract List<BalancerTarget> selectTargets(List<BalancerTarget> targets, String key);

   protected List<BalancerTarget> selectTargetsNext(List<BalancerTarget> targets, String key) {
      if (next == null) {
         return targets;
      }

      return next.selectTargets(targets, key);
   }
}
