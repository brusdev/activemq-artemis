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

import org.apache.activemq.artemis.core.server.balancing.MockTarget;
import org.apache.activemq.artemis.core.server.balancing.targets.Target;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class LeastConnectionsPolicyTest extends BasePolicyTest {

   @Override
   protected Policy createPolicy() {
      return new LeastConnectionsPolicy();
   }

   @Test
   public void testMultipleTargets() {
      final int targetCount = 10;
      Policy policy = createPolicy();
      List<Target> selectedTargets;

      ArrayList<Target> targets = new ArrayList<>();
      for (int i = 0; i < targetCount; i++) {
         targets.add(new MockTarget());
      }

      selectedTargets = policy.selectTargets(targets, "test");
      Assert.assertEquals(targetCount, selectedTargets.size());


      targets.forEach(target -> {
         ((MockTarget)target).setAttributeValue("broker", "ConnectionCount", 3);
         Arrays.stream(policy.getTargetTasks()).forEach(targetTask -> targetTask.call(target));
      });
      selectedTargets = policy.selectTargets(targets, "test");
      Assert.assertEquals(targetCount, selectedTargets.size());


      ((MockTarget)targets.get(0)).setAttributeValue("broker", "ConnectionCount", 2);
      targets.forEach(target -> Arrays.stream(policy.getTargetTasks()).forEach(targetTask -> targetTask.call(target)));
      selectedTargets = policy.selectTargets(targets, "test");
      Assert.assertEquals(1, selectedTargets.size());
      Assert.assertEquals(targets.get(0), selectedTargets.get(0));


      ((MockTarget)targets.get(1)).setAttributeValue("broker", "ConnectionCount", 1);
      ((MockTarget)targets.get(2)).setAttributeValue("broker", "ConnectionCount", 1);
      targets.forEach(target -> Arrays.stream(policy.getTargetTasks()).forEach(targetTask -> targetTask.call(target)));
      selectedTargets = policy.selectTargets(targets, "test");
      Assert.assertEquals(2, selectedTargets.size());
      Assert.assertTrue(selectedTargets.contains(targets.get(1)));
      Assert.assertTrue(selectedTargets.contains(targets.get(2)));
   }
}
