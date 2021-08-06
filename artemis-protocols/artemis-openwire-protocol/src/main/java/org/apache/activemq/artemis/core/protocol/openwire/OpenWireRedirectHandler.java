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

package org.apache.activemq.artemis.core.protocol.openwire;

import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.core.server.balancing.RedirectHandler;
import org.apache.activemq.artemis.core.server.balancing.targets.Target;
import org.apache.activemq.artemis.utils.ConfigurationHelper;
import org.apache.activemq.command.ConnectionControl;
import org.apache.activemq.command.ConnectionInfo;

public class OpenWireRedirectHandler extends RedirectHandler {

   private final OpenWireConnection connection;


   protected OpenWireRedirectHandler(OpenWireConnection connection, ConnectionInfo connectionInfo) {
      super(connection.getServer(), connectionInfo.getClientId(), connectionInfo.getUserName(), connection.getTransportConnection());
      this.connection = connection;
   }

   @Override
   protected void cannotRedirect() throws Exception {
   }

   @Override
   protected void redirectTo(Target target) throws Exception {
      String host = ConfigurationHelper.getStringProperty(TransportConstants.HOST_PROP_NAME, TransportConstants.DEFAULT_HOST, target.getConnector().getParams());
      int port = ConfigurationHelper.getIntProperty(TransportConstants.PORT_PROP_NAME, TransportConstants.DEFAULT_PORT, target.getConnector().getParams());

      ConnectionControl command = connection.getProtocolManager().newConnectionControl();
      command.setConnectedBrokers(String.format("tcp://%s:%d", host, port));
      command.setRebalanceConnection(true);
      connection.dispatchSync(command);
   }
}
