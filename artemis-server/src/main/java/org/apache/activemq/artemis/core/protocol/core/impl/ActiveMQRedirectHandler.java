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

package org.apache.activemq.artemis.core.protocol.core.impl;

import org.apache.activemq.artemis.api.core.DisconnectReason;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.CreateSessionMessage;
import org.apache.activemq.artemis.core.server.ActiveMQMessageBundle;
import org.apache.activemq.artemis.core.server.balancing.RedirectHandler;
import org.apache.activemq.artemis.core.server.balancing.targets.Target;

public class ActiveMQRedirectHandler extends RedirectHandler {
   private final ActiveMQPacketHandler packetHandler;

   public ActiveMQRedirectHandler(ActiveMQPacketHandler packetHandler, CreateSessionMessage request) {
      super(packetHandler.getServer(), packetHandler.getConnection().getClientID(), request.getUsername(), packetHandler.getConnection().getTransportConnection());
      this.packetHandler = packetHandler;
   }

   @Override
   public void cannotRedirect() throws Exception {
      throw ActiveMQMessageBundle.BUNDLE.cannotRedirect();
   }

   @Override
   public void redirectTo(Target target) throws Exception {
      packetHandler.getConnection().disconnect(DisconnectReason.REDIRECT, target.getNodeID(), target.getConnector());

      throw ActiveMQMessageBundle.BUNDLE.redirectConnection(target.getConnector());
   }
}
