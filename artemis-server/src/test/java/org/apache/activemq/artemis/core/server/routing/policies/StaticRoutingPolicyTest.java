/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.core.server.routing.policies;

import org.apache.activemq.artemis.api.core.*;
import org.apache.activemq.artemis.core.server.routing.targets.*;
import org.junit.jupiter.api.*;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class StaticRoutingPolicyTest extends PolicyTestBase {

   @Override
   protected AbstractPolicy createPolicy() {
      return new StaticRoutingPolicy();
   }

   @Test
   public void testPolicyWithMultipleTargets() {
      testPolicyWithMultipleTargets(false, false);
   }

   @Test
   public void testPolicyWithMultipleTargetsWithDefaultRoute() {
      testPolicyWithMultipleTargets(true, false);
   }

   @Test
   public void testPolicyWithMultipleTargetsWithLocalRoute() {
      testPolicyWithMultipleTargets(true, false);
   }

   @Test
   public void testPolicyWithMultipleTargetsWithDefaultAndLocalRoute() {
      testPolicyWithMultipleTargets(true, true);
   }

   private void testPolicyWithMultipleTargets(boolean withDefault, boolean withLocal) {
      AbstractPolicy policy = createPolicy();

      Map<String, String> properties = new HashMap<>();
      if (withDefault) {
         properties.put(StaticRoutingPolicy.DEFAULT_ROUTE, "default");
      }
      if (withLocal) {
         properties.put(StaticRoutingPolicy.LOCAL_ROUTE, "x");
      }
      properties.put(StaticRoutingPolicy.STATIC_ROUTE_PREFIX + "TARGET0", "0");
      properties.put(StaticRoutingPolicy.STATIC_ROUTE_PREFIX + "TARGET1", "1");
      properties.put(StaticRoutingPolicy.STATIC_ROUTE_PREFIX + "TARGET2", "2");

      policy.init(properties);

      Target selectedTarget;

      List<Target> targets = new ArrayList<>();

      TransportConfiguration defaultTransportConfiguration = new TransportConfiguration(null, null, "default", null);
      targets.add(new MockTarget(defaultTransportConfiguration, UUID.randomUUID().toString()));

      TransportConfiguration localTransportConfiguration = defaultTransportConfiguration.newTransportConfig("local");
      Target localTarget = new MockTarget(localTransportConfiguration, UUID.randomUUID().toString()).setLocal(true);
      targets.add(localTarget);

      for (int i = 0; i < MULTIPLE_TARGETS; i++) {
         targets.add(new MockTarget(defaultTransportConfiguration.newTransportConfig("TARGET" + i), UUID.randomUUID().toString()));
      }

      for (int i = 0; i < MULTIPLE_TARGETS; i++) {
         selectedTarget = policy.selectTarget(targets, String.valueOf(i));
         if (i < 3) {
            assertEquals("TARGET" + i, selectedTarget.getConnector().getName());
         } else {
            if (withDefault) {
               assertEquals(defaultTransportConfiguration, selectedTarget.getConnector());
            }
            else {
               assertNull(selectedTarget);
            }
         }
      }

      selectedTarget = policy.selectTarget(targets, "x");
      if (withLocal) {
         assertEquals(localTransportConfiguration, selectedTarget.getConnector());
      } else if (withDefault) {
         assertEquals(defaultTransportConfiguration, selectedTarget.getConnector());
      }
      else {
         assertNull(selectedTarget);
      }

      selectedTarget = policy.selectTarget(targets, "y");
      if (withDefault) {
         assertEquals(defaultTransportConfiguration, selectedTarget.getConnector());
      }
      else {
         assertNull(selectedTarget);
      }
   }
}
