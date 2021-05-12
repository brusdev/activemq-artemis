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
import org.apache.activemq.artemis.core.server.balancing.pools.Pool;

import java.util.Collections;
import java.util.List;

public class FirstElementPolicy extends Policy {
   public static final String NAME = "FIRST_ELEMENT";

   private Pool pool;

   public FirstElementPolicy() {
      super(NAME);
   }

   @Override
   public void load(BrokerBalancer controller) {
      pool = controller.getPool();
   }

   @Override
   public void unload() {

   }

   @Override
   public List<BrokerBalancerTarget> selectTargets(List<BrokerBalancerTarget> targets, String key) {
      if (targets.size() > 1) {
         return selectTargetsNext(Collections.singletonList(targets.get(0)), key);
      } else if (targets.size() > 0) {
         return selectTargetsNext(targets, key);
      }

      return targets;
   }
}
