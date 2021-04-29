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

package org.apache.activemq.artemis.core.server.redirect;

import org.apache.activemq.artemis.api.core.DiscoveryGroupConfiguration;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.cluster.DiscoveryEntry;
import org.apache.activemq.artemis.core.cluster.DiscoveryGroup;
import org.apache.activemq.artemis.core.config.RedirectConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQComponent;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.redirect.algorithms.RedirectAlgorithm;
import org.apache.activemq.artemis.core.server.redirect.algorithms.RedirectAlgorithmFactory;
import org.apache.activemq.artemis.spi.core.security.jaas.RolePrincipal;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RedirectController implements ActiveMQComponent {

   private final ActiveMQServer server;
   private final RedirectConfiguration config;
   private final RedirectAlgorithm algorithm;
   private final RedirectKeyType key;
   private final List<TransportConfiguration> connectors;

   private DiscoveryGroup discoveryGroup;

   public RedirectController(final ActiveMQServer server, final RedirectConfiguration config) {
      this.server = server;
      this.config = config;

      algorithm = RedirectAlgorithmFactory.getAlgorithm(config.getAlgorithm());
      key = config.getKey();

      connectors = new ArrayList<>();
      discoveryGroup = null;
   }

   @Override
   public void start() throws Exception {
      if (config.getDiscoveryGroupName() != null) {
         DiscoveryGroupConfiguration discoveryGroupConfiguration = server.getConfiguration().getDiscoveryGroupConfigurations().get(config.getDiscoveryGroupName());
         discoveryGroup = new DiscoveryGroup(server.getNodeID().toString(), config.getName(), discoveryGroupConfiguration.getRefreshTimeout(), discoveryGroupConfiguration.getBroadcastEndpointFactory(), null);
         discoveryGroup.registerListener(newConnectors -> {
            connectors.clear();
            for (DiscoveryEntry newConnector : newConnectors) {
               connectors.add(newConnector.getConnector());
            }
         });
         discoveryGroup.start();
      } else {
         Map<String, TransportConfiguration> connectorConfigurations =
            server.getConfiguration().getConnectorConfigurations();

         for (String connectorRef : config.getStaticConnectors()) {
            connectors.add(connectorConfigurations.get(connectorRef));
         }
      }
   }

   @Override
   public void stop() throws Exception {
      if (discoveryGroup != null) {
         discoveryGroup.stop();
      }
   }

   @Override
   public boolean isStarted() {
      return false;
   }

   public boolean match(RedirectingConnection connection) {
      return (config.getSourceIP() == null || config.getSourceIP().equals(connection.getSourceIP())) &&
         (config.getUser() == null || config.getUser().equals(connection.getUser())) &&
         (config.getUserRole() == null || new RolePrincipal(config.getUserRole()).implies(connection.getSubject()));
   }

   public TransportConfiguration getConnector(RedirectingConnection connection) {
      return algorithm.selectConnector(connection, connectors);
   }
}
