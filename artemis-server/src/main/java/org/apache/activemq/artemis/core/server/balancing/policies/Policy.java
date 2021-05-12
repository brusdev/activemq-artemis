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

import org.apache.activemq.artemis.core.server.balancing.BrokerBalancer;
import org.apache.activemq.artemis.core.server.balancing.BrokerBalancerTarget;
import org.apache.activemq.artemis.core.server.balancing.pools.PoolTask;

import java.util.List;

public abstract class Policy {
   private final String name;

   private final PoolTask[] poolTasks;

   private Policy next;

   public String getName() {
      return name;
   }

   public PoolTask[] getPoolTasks() {
      return poolTasks;
   }

   public Policy getNext() {
      return next;
   }

   public Policy setNext(Policy next) {
      this.next = next;
      return this;
   }

   public Policy(final String name, final PoolTask[] poolTasks) {
      this.name = name;
      this.poolTasks = poolTasks;
   }

   public abstract List<BrokerBalancerTarget> selectTargets(List<BrokerBalancerTarget> targets, String key);

   protected List<BrokerBalancerTarget> selectNextTargets(List<BrokerBalancerTarget> targets, String key) {
      if (next == null) {
         return targets;
      }

      return next.selectTargets(targets, key);
   }
}
