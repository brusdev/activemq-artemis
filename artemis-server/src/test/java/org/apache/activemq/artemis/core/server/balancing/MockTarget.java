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

package org.apache.activemq.artemis.core.server.balancing;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.server.balancing.targets.AbstractTarget;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class MockTarget extends AbstractTarget {
   private boolean connected = false;

   private boolean ready = false;

   private Map<String, Object> attributeValues = new HashMap<>();

   private Map<String, Object> operationReturns = new HashMap<>();


   @Override
   public boolean isConnected() {
      return connected;
   }

   public MockTarget setConnected(boolean connected) {
      this.connected = connected;
      return this;
   }

   public boolean isReady() {
      return ready;
   }

   public void setReady(boolean ready) {
      this.ready = ready;
   }

   public MockTarget() {
      this(UUID.randomUUID().toString(), new TransportConfiguration());
   }

   public MockTarget(String nodeID, TransportConfiguration connector) {
      super(nodeID, connector);
   }

   @Override
   public void connect() throws Exception {
      connected = true;
   }

   @Override
   public void disconnect() throws Exception {
      connected = false;
   }

   @Override
   public void checkReadiness() throws Exception {
      if (!ready) {
         throw new IllegalStateException("Mock not ready");
      }
   }

   @Override
   public Object getAttribute(String resourceName, String attributeName) throws Exception {
      return attributeValues.get(resourceName + attributeName);
   }

   public void setAttributeValue(String resourceName, String attributeName, Object value) {
      attributeValues.put(resourceName + attributeName, value);
   }

   @Override
   public Object invokeOperation(String resourceName, String operationName, Object... operationArgs) throws Exception {
      return operationReturns.get(resourceName + operationName);
   }

   public void setOperationReturn(String resourceName, String attributeName, Object value) {
      operationReturns.put(resourceName + attributeName, value);
   }
}
