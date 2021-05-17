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

public class FirstElementPolicyTest extends BasePolicyTest {

   @Override
   protected Policy createPolicy() {
      return new FirstElementPolicy();
   }

   @Test
   public void testMultipleTargets() {
      final int targetCount = 10;
      Policy policy = createPolicy();

      ArrayList<Target> targets = new ArrayList<>();
      for (int i = 0; i < targetCount; i++) {
         targets.add(new MockTarget());
      }

      List<Target> selectedTargets = policy.selectTargets(targets, "test");

      Assert.assertEquals(1, selectedTargets.size());
      Assert.assertEquals(selectedTargets.get(0), targets.get(0));
   }
}
