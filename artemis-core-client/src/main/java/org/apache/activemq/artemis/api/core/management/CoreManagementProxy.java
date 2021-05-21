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

package org.apache.activemq.artemis.api.core.management;

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientRequestor;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.management.ManagementProxy;

public class CoreManagementProxy implements ManagementProxy {
   private final ClientSession session;

   public CoreManagementProxy(ClientSession session) {
      this.session = session;
   }

   @Override
   public void connect() throws Exception {

   }

   @Override
   public void disconnect() throws Exception {

   }

   @Override
   public Object getAttribute(String resourceName, String attributeName, int timeout) throws Exception {
      try (ClientRequestor requestor = new ClientRequestor(session, ActiveMQDefaultConfiguration.getDefaultManagementAddress())) {
         ClientMessage request = session.createMessage(false);

         ManagementHelper.putAttribute(request, resourceName, attributeName);

         ClientMessage reply = requestor.request(request, timeout);

         if (ManagementHelper.hasOperationSucceeded(reply)) {
            return ManagementHelper.getResult(reply);
         } else {
            throw new Exception("Failed to get " + resourceName + "." + attributeName + ". Reason: " + ManagementHelper.getResult(reply, String.class));
         }
      }
   }

   @Override
   public Object invokeOperation(String resourceName, String operationName, Object[] operationParams, int timeout) throws Exception {
      try (ClientRequestor requestor = new ClientRequestor(session, ActiveMQDefaultConfiguration.getDefaultManagementAddress())) {
         ClientMessage request = session.createMessage(false);

         ManagementHelper.putOperationInvocation(request, resourceName, operationName, operationParams);

         ClientMessage reply = requestor.request(request, timeout);

         if (ManagementHelper.hasOperationSucceeded(reply)) {
            return ManagementHelper.getResult(reply);
         } else {
            throw new Exception("Failed to invoke " + resourceName + "." + operationName + ". Reason: " + ManagementHelper.getResult(reply, String.class));
         }
      }
   }

   @Override
   public void close() throws Exception {

   }
}
