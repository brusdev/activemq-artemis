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

package org.apache.activemq.artemis.core.server.redirection;

import java.util.HashMap;
import java.util.Map;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.RedirectConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQComponent;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.jboss.logging.Logger;

public class RedirectManager implements ActiveMQComponent {
   private static final Logger logger = Logger.getLogger(RedirectManager.class);

   private final ActiveMQServer server;
   private final Configuration configuration;

   private Map<String, RedirectController> redirectControllers = new HashMap<>();


   public RedirectManager(final ActiveMQServer server, final Configuration configuration) {
      this.server = server;
      this.configuration = configuration;
   }

   public void deploy() {
      for (RedirectConfiguration config : configuration.getRedirectConfigurations()) {
         redirectControllers.put(config.getName(), new RedirectController(config));
      }
   }

   @Override
   public void start() throws Exception {




      //per ogni redirect-setting instanzializza un RedirectController








      //Start DisoveryGroup

   }

   @Override
   public void stop() throws Exception {

   }

   @Override
   public boolean isStarted() {
      return false;
   }

   public TransportConfiguration getRedirectConnector(RedirectingConnection connection) {
      for (RedirectController redirectController : redirectControllers.values()) {
         if (redirectController.match(connection)) {
            return redirectController.getRedirectConnector(connection);
         }
      }

      return  null;
   }
}
