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
package org.apache.activemq.artemis.spi.core.security.jaas;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

import javax.net.SocketFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;

import org.apache.activemq.artemis.core.remoting.impl.ssl.SSLSupport;

/**
 * A custom SocketFactory that uses SSLSupport to create SSL sockets for LDAP connections.
 * This allows LDAPLoginModule to support SSL/TLS configuration similar to other Artemis components.
 * 
 * This factory follows the standard JNDI LDAP socket factory pattern and uses ThreadLocal
 * storage to retrieve the configured SSL support for the current thread.
 */
public class LDAPSocketFactory extends SocketFactory {

   private static final SSLSocketFactory DEFAULT_FACTORY = (SSLSocketFactory) SSLSocketFactory.getDefault();
   private static final LDAPSocketFactory defaultInstance = new LDAPSocketFactory();
   private SSLSocketFactory sslSocketFactory;

   public LDAPSocketFactory() {
      // Default constructor for JNDI
      // This constructor is called by the LDAP JNDI provider
      try {
         SSLSupport sslSupport = LDAPLoginModule.getCurrentSSLSupport();
         if (sslSupport != null) {
            SSLContext sslContext = sslSupport.createContext();
            this.sslSocketFactory = sslContext.getSocketFactory();
         } else {
            // Fallback to default socket factory if not configured
            this.sslSocketFactory = DEFAULT_FACTORY;
         }
      } catch (Exception e) {
         // Fallback to default socket factory if initialization fails
         this.sslSocketFactory = DEFAULT_FACTORY;
      }
   }

   /**
    * Required by JNDI to get the default instance of the socket factory.
    */
   public static LDAPSocketFactory getDefault() {
      return defaultInstance;
   }

   @Override
   public Socket createSocket() throws IOException {
      return sslSocketFactory.createSocket();
   }

   @Override
   public Socket createSocket(String host, int port) throws IOException, UnknownHostException {
      return sslSocketFactory.createSocket(host, port);
   }

   @Override
   public Socket createSocket(String host, int port, InetAddress localHost, int localPort) throws IOException, UnknownHostException {
      return sslSocketFactory.createSocket(host, port, localHost, localPort);
   }

   @Override
   public Socket createSocket(InetAddress host, int port) throws IOException {
      return sslSocketFactory.createSocket(host, port);
   }

   @Override
   public Socket createSocket(InetAddress address, int port, InetAddress localAddress, int localPort) throws IOException {
      return sslSocketFactory.createSocket(address, port, localAddress, localPort);
   }
}

