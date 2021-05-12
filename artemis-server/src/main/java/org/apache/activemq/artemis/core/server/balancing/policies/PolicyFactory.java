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

import java.util.ServiceLoader;

public abstract class PolicyFactory {
   private static final ServiceLoader<PolicyFactory> serviceLoader =
      ServiceLoader.load(PolicyFactory.class, BrokerBalancer.class.getClassLoader());

   public static PolicyFactory forName(String policyName) throws ClassNotFoundException {
      for (PolicyFactory policyFactory : serviceLoader) {
         if (policyFactory.supports(policyName)) {
            return policyFactory;
         }
      }

      throw new ClassNotFoundException("No BalancerPolicyFactory found for the policy " + policyName);
   }

   public static Policy createPolicyForName(String policyName) throws ClassNotFoundException {
      return PolicyFactory.forName(policyName).createPolicy(policyName);
   }

   public abstract String[] getSupportedPolicies();

   public abstract boolean supports(String policyName);

   public abstract Policy createPolicy(String policyName);
}
