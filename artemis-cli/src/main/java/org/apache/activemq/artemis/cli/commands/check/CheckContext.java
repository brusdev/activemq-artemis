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

package org.apache.activemq.artemis.cli.commands.check;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.activemq.artemis.api.core.management.NodeInfo;
import org.apache.activemq.artemis.cli.commands.ActionContext;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.management.ManagementProxy;

import javax.jms.ConnectionFactory;

public class CheckContext extends ActionContext {

   private ActionContext actionContext;
   private ConnectionFactory factory;
   private ManagementProxy managementProxy;

   private String nodeId;
   private Map<String, NodeInfo> topology;

   public ActionContext getActionContext() {
      return actionContext;
   }

   public void setActionContext(ActionContext actionContext) {
      this.actionContext = actionContext;
   }

   public ConnectionFactory getFactory() {
      return factory;
   }

   public void setFactory(ConnectionFactory factory) {
      this.factory = factory;
   }

   public ManagementProxy getManagementProxy() {
      return managementProxy;
   }

   public void setManagementProxy(ManagementProxy managementProxy) {
      this.managementProxy = managementProxy;
   }

   public String getNodeId() throws Exception {
      if (nodeId == null) {
         nodeId = (String)managementProxy.invokeOperation("broker", "getNodeID", null, 0);
      }

      return nodeId;
   }

   public Map<String, NodeInfo> getTopology() throws Exception {
      if (topology == null) {
         topology = Arrays.stream(NodeInfo.from((String)managementProxy.
            invokeOperation("broker", "listNetworkTopology", null, 0))).
            collect(Collectors.toMap(node -> node.getId(), node -> node));
      }

      return topology;
   }

   public CheckContext(ActionContext actionContext, ConnectionFactory factory, ManagementProxy managementProxy) {
      this.actionContext = actionContext;
      this.factory = factory;
      this.managementProxy = managementProxy;
   }
}
