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

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.server.balancing.BrokerBalancerTarget;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class LeastConnectionsPolicyTest extends BasePolicyTest {

   @Override
   protected Policy createPolicy() {
      return new LeastConnectionsPolicy();
   }

   @Test
   public void testMultipleTargets() {
      final int targetCount = 10;
      Policy policy = createPolicy();
      List<BrokerBalancerTarget> selectedTargets;

      ArrayList<BrokerBalancerTarget> targets = new ArrayList<>();
      for (int i = 0; i < targetCount; i++) {
         targets.add(new BrokerBalancerTarget(UUID.randomUUID().toString(), new TransportConfiguration()));
      }

      selectedTargets = policy.selectTargets(targets, "test");
      Assert.assertEquals(targetCount, selectedTargets.size());

      for (BrokerBalancerTarget target : targets) {
         target.setTaskResult(LeastConnectionsPolicy.GET_CONNECTION_COUNT_TASK_NAME, 3);
      }
      selectedTargets = policy.selectTargets(targets, "test");
      Assert.assertEquals(targetCount, selectedTargets.size());


      targets.get(0).setTaskResult(LeastConnectionsPolicy.GET_CONNECTION_COUNT_TASK_NAME, 2);
      selectedTargets = policy.selectTargets(targets, "test");
      Assert.assertEquals(1, selectedTargets.size());
      Assert.assertEquals(targets.get(0), selectedTargets.get(0));

      targets.get(1).setTaskResult(LeastConnectionsPolicy.GET_CONNECTION_COUNT_TASK_NAME, 1);
      targets.get(2).setTaskResult(LeastConnectionsPolicy.GET_CONNECTION_COUNT_TASK_NAME, 1);
      selectedTargets = policy.selectTargets(targets, "test");
      Assert.assertEquals(2, selectedTargets.size());
      Assert.assertTrue(selectedTargets.contains(targets.get(1)));
      Assert.assertTrue(selectedTargets.contains(targets.get(2)));
   }
}
