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
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public abstract class BasePolicyTest {

   protected abstract Policy createPolicy();

   @Test
   public void testNoTarget() {
      Policy policy = createPolicy();

      List<BrokerBalancerTarget> selectedTargets = policy.selectTargets(Collections.emptyList(), "test");

      Assert.assertEquals(0, selectedTargets.size());
   }

   @Test
   public void testSingleTarget() {
      Policy policy = createPolicy();

      ArrayList<BrokerBalancerTarget> targets = new ArrayList<>();
      targets.add(new BrokerBalancerTarget(UUID.randomUUID().toString(), new TransportConfiguration()));

      List<BrokerBalancerTarget> selectedTargets = policy.selectTargets(targets, "test");

      Assert.assertEquals(1, selectedTargets.size());
      Assert.assertEquals(selectedTargets.get(0), targets.get(0));
   }
}
