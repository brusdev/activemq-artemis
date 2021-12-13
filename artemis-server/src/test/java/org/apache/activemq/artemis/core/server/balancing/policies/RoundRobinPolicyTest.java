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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@RunWith(value = Parameterized.class)
public class RoundRobinPolicyTest extends PolicyTestBase {

   @Parameterized.Parameters(name = "randomize: {0}")
   public static Collection<Object[]> data() {
      return Arrays.asList(new Object[][]{new Object[] {true}, new Object[] {false}});
   }

   private final boolean randomize;

   public RoundRobinPolicyTest(boolean randomize) {
      this.randomize = randomize;
   }

   @Override
   protected AbstractPolicy createPolicy() {
      RoundRobinPolicy roundRobinPolicy = new RoundRobinPolicy();
      roundRobinPolicy.init(Collections.singletonMap(RoundRobinPolicy.RANDOMIZE, Boolean.toString(randomize)));
      return roundRobinPolicy;
   }

   @Test
   public void testPolicyWithMultipleTargets() {
      AbstractPolicy policy = createPolicy();
      Target selectedTarget = null;
      Set<Target> selectedTargets;
      List<Target> previousTargets = new ArrayList<>();

      ArrayList<Target> targets = new ArrayList<>();
      for (int i = 0; i < MULTIPLE_TARGETS; i++) {
         targets.add(new MockTarget());
      }

      selectedTargets = new HashSet<>();
      for (int i = 0; i < MULTIPLE_TARGETS; i++) {
         selectedTarget = policy.selectTarget(targets, "test");
         selectedTargets.add(selectedTarget);
         Assert.assertTrue("Iteration failed: " + i, !previousTargets.contains(selectedTarget));
         previousTargets.add(selectedTarget);
      }
      Assert.assertEquals(MULTIPLE_TARGETS, selectedTargets.size());
   }
}
