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

import org.apache.activemq.artemis.core.server.balancer.BalancerController;

import java.util.ServiceLoader;

public abstract class BalancerPolicyFactory {
   private static final ServiceLoader<BalancerPolicyFactory> serviceLoader =
      ServiceLoader.load(BalancerPolicyFactory.class, BalancerController.class.getClassLoader());

   public static BalancerPolicyFactory forName(String policyName) throws ClassNotFoundException {
      for (BalancerPolicyFactory policyFactory : serviceLoader) {
         if (policyFactory.supports(policyName)) {
            return policyFactory;
         }
      }

      throw new ClassNotFoundException("No BalancerPolicyFactory found for the policy " + policyName);
   }

   public static BalancerPolicy policyForName(String policyName) throws ClassNotFoundException {
      return BalancerPolicyFactory.forName(policyName).createPolicy(policyName);
   }

   public abstract String[] getSupportedPolicies();

   public abstract boolean supports(String name);

   public abstract BalancerPolicy createPolicy(String name);
}
