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

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.config.RedirectConfiguration;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.spi.core.security.jaas.RolePrincipal;

public class RedirectController {

   private final RedirectConfiguration config;

   public RedirectController(final RedirectConfiguration config) {
      this.config = config;
   }

   public TransportConfiguration getRedirectConnector(RedirectingConnection connection) {








      HashMap<String, Object> params = new HashMap<>();
      params.put(TransportConstants.HOST_PROP_NAME, TransportConstants.DEFAULT_HOST);
      params.put(TransportConstants.PORT_PROP_NAME, TransportConstants.DEFAULT_PORT);
      return new TransportConfiguration(null, params);
   }

   public boolean match(RedirectingConnection connection) {
      return config.getSourceIP() != null && config.getSourceIP().equals(connection.getSourceIP()) &&
         config.getUser() != null && config.getUser().equals(connection.getUser()) &&
         config.getUserRole() != null && new RolePrincipal(config.getUserRole()).implies(connection.getSubject());
   }
}
