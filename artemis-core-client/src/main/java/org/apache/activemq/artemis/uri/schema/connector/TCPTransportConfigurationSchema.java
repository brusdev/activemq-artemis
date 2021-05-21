/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.uri.schema.connector;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyConnectorFactory;
import org.apache.activemq.artemis.utils.IPV6Util;
import org.apache.activemq.artemis.utils.uri.BeanSupport;
import org.apache.activemq.artemis.utils.uri.SchemaConstants;

public class TCPTransportConfigurationSchema extends AbstractTransportConfigurationSchema {

   private final Set<String> allowableProperties;

   public TCPTransportConfigurationSchema(Set<String> allowableProperties) {
      this.allowableProperties = allowableProperties;
   }

   @Override
   public String getSchemaName() {
      return SchemaConstants.TCP;
   }

   @Override
   protected List<TransportConfiguration> internalNewObject(URI uri,
                                                            Map<String, String> query,
                                                            String name) throws Exception {
      return getTransportConfigurations(uri, query, allowableProperties, name, getFactoryName(uri));
   }

   @Override
   protected URI internalNewURI(List<TransportConfiguration> bean) throws Exception {
      if (bean.size() < 1) {
         throw new Exception();
      }
      StringBuilder fragment = new StringBuilder();
      for (int i = 1; i < bean.size(); i++) {
         TransportConfiguration connector = bean.get(i);
         Map<String, Object> params = escapeIPv6Host(connector.getParams());
         URI extraUri = new URI(SchemaConstants.TCP, null, getHost(params), getPort(params), null, createQuery(params), null);
         if (i > 1) {
            fragment.append(",");
         }
         fragment.append(extraUri.toASCIIString());

      }
      Map<String, Object> params = escapeIPv6Host(bean.get(0).getParams());
      return new URI(SchemaConstants.TCP, null, getHost(params), getPort(params), null, createQuery(params), fragment.toString());
   }

   @SuppressWarnings("StringEquality")
   private static Map<String, Object> escapeIPv6Host(Map<String, Object> params) {
      String host = (String) params.get("host");
      String newHost = IPV6Util.encloseHost(host);

      // We really want to check the objects here
      // Some bug finders may report this as an error, hence the SuppressWarnings on this method
      if (host != newHost) {
         params.put("host", "[" + host + "]");
      }

      return params;
   }

   private static int getPort(Map<String, Object> params) {
      Object port = params.get("port");
      if (port instanceof String) {
         return Integer.valueOf((String) port);
      }
      return port != null ? (int) port : 61616;
   }

   private static String getHost(Map<String, Object> params) {
      return params.get("host") != null ? (String) params.get("host") : "localhost";
   }

   private static String createQuery(Map<String, Object> params) throws Exception {
      StringBuilder cb = new StringBuilder();
      boolean empty = true;
      for (Map.Entry<String, Object> entry : params.entrySet()) {
         if (entry.getValue() != null) {
            if (!empty) {
               cb.append("&");
            } else {
               empty = false;
            }
            cb.append(BeanSupport.encodeURI(entry.getKey()));
            cb.append("=");
            cb.append(BeanSupport.encodeURI(entry.getValue().toString()));
         }
      }
      return cb.toString();
   }

   public static List<TransportConfiguration> getTransportConfigurations(URI uri,
                                                                         Map<String, String> query,
                                                                         Set<String> allowableProperties,
                                                                         String name,
                                                                         String factoryName) throws URISyntaxException {
      HashMap<String, Object> props = new HashMap<>();

      Map<String, Object> extraProps = new HashMap<>();
      BeanSupport.setData(uri, props, allowableProperties, query, extraProps);
      List<TransportConfiguration> transportConfigurations = new ArrayList<>();

      TransportConfiguration config = new TransportConfiguration(factoryName, props, name, extraProps);

      transportConfigurations.add(config);
      String connectors = uri.getFragment();

      if (connectors != null && !connectors.trim().isEmpty()) {
         String[] split = connectors.split(",");
         for (String s : split) {
            URI extraUri = new URI(s);
            HashMap<String, Object> newProps = new HashMap<>();
            extraProps = new HashMap<>();
            BeanSupport.setData(extraUri, newProps, allowableProperties, query, extraProps);
            BeanSupport.setData(extraUri, newProps, allowableProperties, parseQuery(extraUri.getQuery(), null), extraProps);
            transportConfigurations.add(new TransportConfiguration(factoryName, newProps, name + ":" + extraUri.toString(), extraProps));
         }
      }
      return transportConfigurations;
   }

   protected String getFactoryName(URI uri) {
      //here for backwards compatibility
      if (uri.getPath() != null && uri.getPath().contains("hornetq")) {
         return "org.hornetq.core.remoting.impl.netty.NettyConnectorFactory";
      }
      return NettyConnectorFactory.class.getName();
   }
}
