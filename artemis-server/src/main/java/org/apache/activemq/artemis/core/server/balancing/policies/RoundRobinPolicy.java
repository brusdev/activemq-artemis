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

import org.apache.activemq.artemis.core.server.balancing.BrokerBalancerTarget;
import org.apache.activemq.artemis.utils.RandomUtil;

import java.util.Collections;
import java.util.List;

public class RoundRobinPolicy extends Policy {
   public static final String NAME = "ROUND_ROBIN";

   private int pos = RandomUtil.randomInterval(0, Integer.MAX_VALUE);

   public RoundRobinPolicy() {
      super(NAME, null);
   }

   @Override
   public List<BrokerBalancerTarget> selectTargets(List<BrokerBalancerTarget> targets, String key) {
      if (targets.size() > 1) {
         pos = pos % targets.size();
         return selectNextTargets(Collections.singletonList(targets.get(pos++)), key);
      } else if (targets.size() > 0) {
         return selectNextTargets(targets, key);
      }

      return targets;
   }
}
