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

package org.apache.activemq.artemis.core.server.balancing.targets;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.utils.JsonLoader;

import javax.json.JsonObject;
import java.io.Serializable;

public class TargetReference implements Serializable {
   private final String nodeID;
   private final TransportConfiguration connector;

   public String getNodeID() {
      return nodeID;
   }

   public TransportConfiguration getConnector() {
      return connector;
   }

   public TargetReference(String nodeID, TransportConfiguration connector) {
      this.nodeID = nodeID;
      this.connector = connector;
   }

   public JsonObject toJson() {
      return JsonLoader.createObjectBuilder().add("nodeID", nodeID).add("connector", connector.toJson()).build();
   }

   @Override
   public String toString() {
      StringBuilder stringBuilder = new StringBuilder(Target.class.getSimpleName());
      stringBuilder.append("(nodeID=" + nodeID);
      stringBuilder.append(", connector=" + connector);
      stringBuilder.append(") ");
      return stringBuilder.toString();
   }
}
