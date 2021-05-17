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

import org.apache.activemq.artemis.core.server.balancing.targets.MockTarget;
import org.apache.activemq.artemis.core.server.balancing.targets.Target;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class RoundRobinPolicyTest extends BasePolicyTest {

   @Override
   protected Policy createPolicy() {
      return new RoundRobinPolicy();
   }

   @Test
   public void testMultipleTargets() {
      final int targetCount = 10;
      Policy policy = createPolicy();
      List<Target> selectedTargets;
      List<Target> previousTargets = new ArrayList<>();

      ArrayList<Target> targets = new ArrayList<>();
      for (int i = 0; i < targetCount; i++) {
         targets.add(new MockTarget());
      }

      for (int i = 0; i < targetCount; i++) {
         selectedTargets = policy.selectTargets(targets, "test");
         Assert.assertEquals(1, selectedTargets.size());
         Assert.assertTrue("Iteration failed: " + i, !previousTargets.contains(selectedTargets.get(0)));
         previousTargets.add(selectedTargets.get(0));
      }
   }
}
