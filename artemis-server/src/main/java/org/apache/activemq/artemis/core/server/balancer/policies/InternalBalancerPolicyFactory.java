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

package org.apache.activemq.artemis.core.server.balancer.policies;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

public class InternalBalancerPolicyFactory extends BalancerPolicyFactory {
   private static final Map<String, Supplier<BalancerPolicy>> supportedPolicies = new HashMap<>();

   static {
      supportedPolicies.put(ConsistentHashBalancerPolicy.NAME, () -> new ConsistentHashBalancerPolicy());
      supportedPolicies.put(FirstElementBalancerPolicy.NAME, () -> new FirstElementBalancerPolicy());
      supportedPolicies.put(LeastConnectionsBalancerPolicy.NAME, () -> new LeastConnectionsBalancerPolicy());
      supportedPolicies.put(RoundRobinBalancerPolicy.NAME, () -> new RoundRobinBalancerPolicy());
   }

   @Override
   public String[] getSupportedPolicies() {
      return supportedPolicies.keySet().toArray(new String[supportedPolicies.size()]);
   }

   @Override
   public boolean supports(String name) {
      return supportedPolicies.containsKey(name);
   }

   @Override
   public BalancerPolicy createPolicy(String name) {
      Supplier<BalancerPolicy> policySupplier = supportedPolicies.get(name);

      if (policySupplier == null) {
         throw new IllegalStateException("Unexpected value: " + name);
      }

      return policySupplier.get();
   }
}
