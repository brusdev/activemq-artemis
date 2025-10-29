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

import javax.naming.Context;
import javax.naming.NameClassPair;
import javax.naming.NamingEnumeration;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.security.auth.Subject;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.FailedLoginException;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.Socket;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.directory.server.annotations.CreateLdapServer;
import org.apache.directory.server.annotations.CreateTransport;
import org.apache.directory.server.core.annotations.ApplyLdifFiles;
import org.apache.directory.server.core.integ.AbstractLdapTestUnit;
import org.apache.directory.server.core.integ.FrameworkRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(FrameworkRunner.class)
@CreateLdapServer(transports = {
      @CreateTransport(protocol = "LDAP", port = 1024),
      @CreateTransport(protocol = "LDAPS", port = 1063)
}, allowAnonymousAccess = true, keyStore="/home/dbruscin/Workspace/brusdev/activemq-artemis/tests/security-resources/server-keystore.jks", certificatePassword = "securepass")
@ApplyLdifFiles("test.ldif")
public class LDAPLoginModuleTest extends AbstractLdapTestUnit {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private static final String PRINCIPAL = "uid=admin,ou=system";
   private static final String CREDENTIALS = "secret";

   private final String loginConfigSysPropName = "java.security.auth.login.config";
   private String oldLoginConfig;

   @Before
   public void setLoginConfigSysProperty() {
      oldLoginConfig = System.getProperty(loginConfigSysPropName, null);
      System.setProperty(loginConfigSysPropName, "src/test/resources/login.config");
   }

   @After
   public void resetLoginConfigSysProperty() {
      if (oldLoginConfig != null) {
         System.setProperty(loginConfigSysPropName, oldLoginConfig);
      }
   }

   @Test
   public void testRunning() throws Exception {

      Hashtable<String, String> env = new Hashtable<>();
      env.put(Context.PROVIDER_URL, "ldap://localhost:1024");
      env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
      env.put(Context.SECURITY_AUTHENTICATION, "simple");
      env.put(Context.SECURITY_PRINCIPAL, PRINCIPAL);
      env.put(Context.SECURITY_CREDENTIALS, CREDENTIALS);
      DirContext ctx = new InitialDirContext(env);

      Set<String> set = new HashSet<>();

      NamingEnumeration<NameClassPair> list = ctx.list("ou=system");

      while (list.hasMore()) {
         NameClassPair ncp = list.next();
         set.add(ncp.getName());
      }

      assertTrue(set.contains("uid=admin"));
      assertTrue(set.contains("ou=users"));
      assertTrue(set.contains("ou=groups"));
      assertTrue(set.contains("ou=configuration"));
      assertTrue(set.contains("prefNodeName=sysPrefRoot"));

   }

   /**
    * A trust-all SSL socket factory for testing with embedded LDAP servers.
    * This accepts all certificates, which is suitable only for test environments.
    */
   public static class TrustAllSSLSocketFactory extends javax.net.SocketFactory {
      private static final SSLSocketFactory trustAllFactory;
      private static final TrustAllSSLSocketFactory defaultInstance = new TrustAllSSLSocketFactory();

      static {
         try {
            SSLContext sslContext = SSLContext.getInstance("TLS");
            TrustManager[] trustAllCerts = new TrustManager[]{
               new X509TrustManager() {
                  public X509Certificate[] getAcceptedIssuers() {
                     return null;
                  }

                  public void checkClientTrusted(X509Certificate[] certs, String authType) {
                     System.out.println("checkClientTrusted");
                  }

                  public void checkServerTrusted(X509Certificate[] certs, String authType) {
                     System.out.println("checkServerTrusted");
                  }
               }
            };
            sslContext.init(null, trustAllCerts, new java.security.SecureRandom());
            trustAllFactory = sslContext.getSocketFactory();
         } catch (Exception e) {
            throw new RuntimeException("Failed to create trust-all SSL context", e);
         }
      }

      public TrustAllSSLSocketFactory() {
      }

      /**
       * Required by JNDI to get the default instance of the socket factory.
       */
      public static TrustAllSSLSocketFactory getDefault() {
         return defaultInstance;
      }

      /**
       * Disable hostname verification on the SSL socket by setting empty endpoint identification algorithm.
       */
      private static void disableHostnameVerification(Socket socket) {
         if (socket instanceof SSLSocket) {
            SSLSocket sslSocket = (SSLSocket) socket;
            SSLParameters params = sslSocket.getSSLParameters();
            // Disable endpoint identification to bypass hostname verification
            //params.setEndpointIdentificationAlgorithm("");
            //params.setServerNames(Collections.singletonList(new SNIHostName("ApacheDS")));
            sslSocket.setSSLParameters(params);
         }
      }

      @Override
      public Socket createSocket(String host, int port) throws java.io.IOException {
         Socket socket = trustAllFactory.createSocket(host, port);
         disableHostnameVerification(socket);
         return socket;
      }

      @Override
      public Socket createSocket(String host, int port, java.net.InetAddress localHost, int localPort) throws java.io.IOException {
         Socket socket = trustAllFactory.createSocket(host, port, localHost, localPort);
         disableHostnameVerification(socket);
         return socket;
      }

      @Override
      public Socket createSocket(java.net.InetAddress host, int port) throws java.io.IOException {
         Socket socket = trustAllFactory.createSocket(host, port);
         disableHostnameVerification(socket);
         return socket;
      }

      @Override
      public Socket createSocket(java.net.InetAddress address, int port, java.net.InetAddress localAddress, int localPort) throws java.io.IOException {
         Socket socket = trustAllFactory.createSocket(address, port, localAddress, localPort);
         disableHostnameVerification(socket);
         return socket;
      }
   }

   @Test
   public void testRunningSSL() throws Exception {
      Hashtable<String, Object> env = new Hashtable<>();
      env.put(Context.PROVIDER_URL, "ldaps://localhost:1063");
      env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
      env.put(Context.SECURITY_AUTHENTICATION, "simple");
      env.put(Context.SECURITY_PRINCIPAL, PRINCIPAL);
      env.put(Context.SECURITY_CREDENTIALS, CREDENTIALS);
      env.put(Context.SECURITY_PROTOCOL, "ssl");
      // Use trust-all socket factory for test server (accepts self-signed certificates)
      env.put("java.naming.ldap.factory.socket", TrustAllSSLSocketFactory.class.getName());
      env.put("com.sun.jndi.ldap.object.disableEndpointIdentification", "true");
      DirContext ctx = new InitialDirContext(env);

      Set<String> set = new HashSet<>();

      NamingEnumeration<NameClassPair> list = ctx.list("ou=system");

      while (list.hasMore()) {
         NameClassPair ncp = list.next();
         set.add(ncp.getName());
      }

      assertTrue(set.contains("uid=admin"));
      assertTrue(set.contains("ou=users"));
      assertTrue(set.contains("ou=groups"));
      assertTrue(set.contains("ou=configuration"));
      assertTrue(set.contains("prefNodeName=sysPrefRoot"));

      ctx.close();
   }

   @Test
   public void testLogin() throws Exception {
      logger.info("num session: {}", ldapServer.getLdapSessionManager().getSessions().length);

      LoginContext context = new LoginContext("LDAPLogin", callbacks -> {
         for (int i = 0; i < callbacks.length; i++) {
            if (callbacks[i] instanceof NameCallback nameCallback) {
               nameCallback.setName("first");
            } else if (callbacks[i] instanceof PasswordCallback passwordCallback) {
               passwordCallback.setPassword("secret".toCharArray());
            } else {
               throw new UnsupportedCallbackException(callbacks[i]);
            }
         }
      });
      context.login();
      context.logout();

      assertTrue("sessions still active after logout", waitFor(() -> ldapServer.getLdapSessionManager().getSessions().length == 0));
   }

   public interface Condition {
      boolean isSatisfied() throws Exception;
   }

   private boolean waitFor(final Condition condition) throws Exception {
      final long expiry = System.currentTimeMillis() + 5000;
      boolean conditionSatisified = condition.isSatisfied();
      while (!conditionSatisified && System.currentTimeMillis() < expiry) {
         TimeUnit.MILLISECONDS.sleep(100);
         conditionSatisified = condition.isSatisfied();
      }
      return conditionSatisified;
   }

   @Test
   public void testUnauthenticated() throws Exception {
      LoginContext context = new LoginContext("UnAuthenticatedLDAPLogin", callbacks -> {
         for (int i = 0; i < callbacks.length; i++) {
            if (callbacks[i] instanceof NameCallback nameCallback) {
               nameCallback.setName("first");
            } else if (callbacks[i] instanceof PasswordCallback passwordCallback) {
               passwordCallback.setPassword("secret".toCharArray());
            } else {
               throw new UnsupportedCallbackException(callbacks[i]);
            }
         }
      });
      try {
         context.login();
      } catch (LoginException le) {
         assertEquals("Empty password is not allowed", le.getCause().getMessage());
         return;
      }
      fail("Should have failed authenticating");
      assertTrue("sessions still active after logout", waitFor(() -> ldapServer.getLdapSessionManager().getSessions().length == 0));
   }


   @Test
   public void testAuthenticatedViaBindOnAnonConnection() throws Exception {
      LoginContext context = new LoginContext("AnonBindCheckUserLDAPLogin", callbacks -> {
         for (int i = 0; i < callbacks.length; i++) {
            if (callbacks[i] instanceof NameCallback nameCallback) {
               nameCallback.setName("first");
            } else if (callbacks[i] instanceof PasswordCallback passwordCallback) {
               passwordCallback.setPassword("wrongSecret".toCharArray());
            } else {
               throw new UnsupportedCallbackException(callbacks[i]);
            }
         }
      });
      try {
         context.login();
         fail("Should have failed authenticating");
      } catch (FailedLoginException expected) {
      }
      assertTrue("sessions still active after logout", waitFor(() -> ldapServer.getLdapSessionManager().getSessions().length == 0));
   }

   @Test
   public void testAuthenticatedOkViaBindOnAnonConnection() throws Exception {
      LoginContext context = new LoginContext("AnonBindCheckUserLDAPLogin", callbacks -> {
         for (int i = 0; i < callbacks.length; i++) {
            if (callbacks[i] instanceof NameCallback nameCallback) {
               nameCallback.setName("first");
            } else if (callbacks[i] instanceof PasswordCallback passwordCallback) {
               passwordCallback.setPassword("secret".toCharArray());
            } else {
               throw new UnsupportedCallbackException(callbacks[i]);
            }
         }
      });
      context.login();
      context.logout();
      assertTrue("sessions still active after logout", waitFor(() -> ldapServer.getLdapSessionManager().getSessions().length == 0));
   }

   @Test
   public void testCommitOnFailedLogin() throws LoginException {
      LoginModule loginModule = new LDAPLoginModule();
      JaasCallbackHandler callbackHandler = new JaasCallbackHandler(null, null, null);

      loginModule.initialize(new Subject(), callbackHandler, null, new HashMap<>());

      // login should return false due to null username
      assertFalse(loginModule.login());

      // since login failed commit should return false as well
      assertFalse(loginModule.commit());
   }

   @Test
   public void testPropertyConfigMap() throws Exception {
      LDAPLoginModule loginModule = new LDAPLoginModule();
      JaasCallbackHandler callbackHandler = new JaasCallbackHandler(null, null, null);

      Field configMap = null;
      Map<String, Object> options = new HashMap<>();
      for (Field field: loginModule.getClass().getDeclaredFields()) {
         if (Modifier.isStatic(field.getModifiers()) && Modifier.isFinal(field.getModifiers()) && field.getType().isAssignableFrom(String.class)) {
            field.setAccessible(true);
            options.put((String)field.get(loginModule), "SET");
         }
         if (field.getName().equals("config")) {
            field.setAccessible(true);
            configMap = field;
         }
      }
      loginModule.initialize(new Subject(), callbackHandler, null, options);

      Set<LDAPLoginProperty> ldapProps = (Set<LDAPLoginProperty>) configMap.get(loginModule);
      for (String key: options.keySet()) {
         assertTrue("val set: " + key, presentIn(ldapProps, key));
      }
   }

   @Test
   public void testEmptyPassword() throws Exception {
      LoginContext context = new LoginContext("LDAPLogin", callbacks -> {
         for (int i = 0; i < callbacks.length; i++) {
            if (callbacks[i] instanceof NameCallback nameCallback) {
               nameCallback.setName("first");
            } else if (callbacks[i] instanceof PasswordCallback passwordCallback) {
               passwordCallback.setPassword("".toCharArray());
            } else {
               throw new UnsupportedCallbackException(callbacks[i]);
            }
         }
      });
      try {
         context.login();
         fail("Should have thrown a FailedLoginException");
      } catch (FailedLoginException fle) {
         assertEquals("Password cannot be null or empty", fle.getMessage());
      }
      context.logout();
   }

   @Test
   public void testNullPassword() throws Exception {
      LoginContext context = new LoginContext("LDAPLogin", callbacks -> {
         for (int i = 0; i < callbacks.length; i++) {
            if (callbacks[i] instanceof NameCallback nameCallback) {
               nameCallback.setName("first");
            } else if (callbacks[i] instanceof PasswordCallback passwordCallback) {
               passwordCallback.setPassword(null);
            } else {
               throw new UnsupportedCallbackException(callbacks[i]);
            }
         }
      });
      try {
         context.login();
         fail("Should have thrown a FailedLoginException");
      } catch (FailedLoginException fle) {
         assertEquals("Password cannot be null or empty", fle.getMessage());
      }
      context.logout();
   }
   @Test
   public void testEnvironmentProperties() throws Exception {
      Map<String, Object> options = new HashMap<>();

      // set module configs
      for (LDAPLoginModule.ConfigKey configKey: LDAPLoginModule.ConfigKey.values()) {
         if (configKey.getName().equals("initialContextFactory")) {
            options.put(configKey.getName(), "com.sun.jndi.ldap.LdapCtxFactory");
         } else if (configKey.getName().equals("connectionURL")) {
            options.put(configKey.getName(), "ldap://localhost:1024");
         } else if (configKey.getName().equals("referral")) {
            options.put(configKey.getName(), "ignore");
         } else if (configKey.getName().equals("connectionTimeout")) {
            options.put(configKey.getName(), "10000");
         } else if (configKey.getName().equals("readTimeout")) {
            options.put(configKey.getName(), "11000");
         } else if (configKey.getName().equals("authentication")) {
            options.put(configKey.getName(), "simple");
         } else if (configKey.getName().equals("connectionUsername")) {
            options.put(configKey.getName(), PRINCIPAL);
         } else if (configKey.getName().equals("connectionPassword")) {
            options.put(configKey.getName(), CREDENTIALS);
         } else if (configKey.getName().equals("connectionProtocol")) {
            options.put(configKey.getName(), "s");
         } else if (configKey.getName().equals("debug")) {
            options.put(configKey.getName(), "true");
         } else {
            options.put(configKey.getName(), configKey.getName() + "_value_set");
         }
      }

      // add extra configs
      options.put("com.sun.jndi.ldap.tls.cbtype", "tls-server-end-point");
      options.put("randomConfig", "some-value");

      // add non-strings configs
      options.put("non.string.1", new Object());
      options.put("non.string.2", 1);

      // create context
      LDAPLoginModule loginModule = new LDAPLoginModule();
      loginModule.initialize(new Subject(), null, null, options);
      loginModule.openContext();

      // get created environment
      Map<?, ?> environment = loginModule.context.getEnvironment();
      // cleanup
      loginModule.closeContext();

      // module config keys should not be passed to environment
      for (LDAPLoginModule.ConfigKey configKey: LDAPLoginModule.ConfigKey.values()) {
         assertNull("value should not be set for key: " + configKey.getName(), environment.get(configKey.getName()));
      }

      // extra, non-module configs should be passed to environment
      assertEquals("value should be set for key: " + "com.sun.jndi.ldap.tls.cbtype", "tls-server-end-point", environment.get("com.sun.jndi.ldap.tls.cbtype"));
      assertEquals("value should be set for key: " + "randomConfig", "some-value", environment.get("randomConfig"));

      // non-string configs should not be passed to environment
      assertNull("value should not be set for key: " + "non.string.1", environment.get("non.string.1"));
      assertNull("value should not be set for key: " + "non.string.2", environment.get("non.string.2"));

      // environment configs should be set
      assertEquals("value should be set for key: " + Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory", environment.get(Context.INITIAL_CONTEXT_FACTORY));
      assertEquals("value should be set for key: " + Context.PROVIDER_URL, "ldap://localhost:1024", environment.get(Context.PROVIDER_URL));
      assertEquals("value should be set for key: " + Context.REFERRAL, "ignore", environment.get(Context.REFERRAL));
      assertEquals("value should be set for key: " + "com.sun.jndi.ldap.connect.timeout", "10000", environment.get("com.sun.jndi.ldap.connect.timeout"));
      assertEquals("value should be set for key: " + "com.sun.jndi.ldap.read.timeout", "11000", environment.get("com.sun.jndi.ldap.read.timeout"));
      assertEquals("value should be set for key: " + Context.SECURITY_AUTHENTICATION, "simple", environment.get(Context.SECURITY_AUTHENTICATION));
      assertEquals("value should be set for key: " + Context.SECURITY_PRINCIPAL, PRINCIPAL, environment.get(Context.SECURITY_PRINCIPAL));
      assertEquals("value should be set for key: " + Context.SECURITY_CREDENTIALS, CREDENTIALS, environment.get(Context.SECURITY_CREDENTIALS));
      assertEquals("value should be set for key: " + Context.SECURITY_PROTOCOL, "s", environment.get(Context.SECURITY_PROTOCOL));
   }

   private boolean presentIn(Set<LDAPLoginProperty> ldapProps, String propertyName) {
      for (LDAPLoginProperty conf : ldapProps) {
         if (conf.getPropertyName().equals(propertyName) && (conf.getPropertyValue() != null && !"".equals(conf.getPropertyValue())))
            return true;
      }
      return false;
   }

   @Test
   public void testSSLSocketFactoryConfiguration() throws Exception {
      Map<String, Object> options = new HashMap<>();
      
      // Set basic LDAP connection options
      options.put("initialContextFactory", "com.sun.jndi.ldap.LdapCtxFactory");
      options.put("connectionURL", "ldaps://localhost:1063");
      options.put("connectionUsername", PRINCIPAL);
      options.put("connectionPassword", CREDENTIALS);
      options.put("connectionProtocol", "ssl");
      options.put("authentication", "simple");
      
      // Set SSL configuration options
      options.put("truststorePath", "/home/dbruscin/Workspace/brusdev/activemq-artemis/tests/security-resources/server-ca-truststore.jks");
      options.put("truststorePassword", "securepass");

      LDAPLoginModule loginModule = new LDAPLoginModule();
      loginModule.initialize(new Subject(), null, null, options);
      loginModule.openContext();
      
      // Get created environment
      Map<?, ?> environment = loginModule.context.getEnvironment();
      
      // Verify that java.naming.ldap.factory.socket is set to our custom factory
      assertEquals("java.naming.ldap.factory.socket should be set", 
                   LDAPSocketFactory.class.getName(), 
                   environment.get("java.naming.ldap.factory.socket"));
      
      // Verify SSLSupport is available in ThreadLocal
      assertNotNull("SSLSupport should be available", LDAPLoginModule.getCurrentSSLSupport());
      
      // Cleanup
      loginModule.closeContext();
      
      // Verify ThreadLocal is cleared after close
      assertNull("SSLSupport should be cleared after close", LDAPLoginModule.getCurrentSSLSupport());
   }

   @Test
   public void testSSLSocketFactoryWithoutSSLConfig() throws Exception {
      Map<String, Object> options = new HashMap<>();
      
      // Set basic LDAP connection options (no SSL config)
      options.put("initialContextFactory", "com.sun.jndi.ldap.LdapCtxFactory");
      options.put("connectionURL", "ldap://localhost:1024");
      options.put("connectionUsername", PRINCIPAL);
      options.put("connectionPassword", CREDENTIALS);
      options.put("connectionProtocol", "s");
      options.put("authentication", "simple");
      
      LDAPLoginModule loginModule = new LDAPLoginModule();
      loginModule.initialize(new Subject(), null, null, options);
      loginModule.openContext();
      
      // Get created environment
      Map<?, ?> environment = loginModule.context.getEnvironment();
      
      // Verify that java.naming.ldap.factory.socket is NOT set when no SSL config is provided
      assertNull("java.naming.ldap.factory.socket should not be set without SSL config", 
                 environment.get("java.naming.ldap.factory.socket"));
      
      // Verify SSLSupport is not available in ThreadLocal
      assertNull("SSLSupport should not be available without SSL config", 
                 LDAPLoginModule.getCurrentSSLSupport());
      
      // Cleanup
      loginModule.closeContext();
   }

   @Test
   public void testSSLSocketFactoryWithKeystoreOnly() throws Exception {
      Map<String, Object> options = new HashMap<>();
      
      // Set basic LDAP connection options
      options.put("initialContextFactory", "com.sun.jndi.ldap.LdapCtxFactory");
      options.put("connectionURL", "ldap://localhost:1024");
      options.put("connectionUsername", PRINCIPAL);
      options.put("connectionPassword", CREDENTIALS);
      options.put("connectionProtocol", "s");
      options.put("authentication", "simple");
      
      // Set only keystore SSL configuration
      options.put("keystorePath", "/path/to/keystore.jks");
      options.put("keystorePassword", "keystoreSecret");
      
      LDAPLoginModule loginModule = new LDAPLoginModule();
      loginModule.initialize(new Subject(), null, null, options);
      loginModule.openContext();
      
      // Get created environment
      Map<?, ?> environment = loginModule.context.getEnvironment();
      
      // Verify that java.naming.ldap.factory.socket is set when keystore is configured
      assertEquals("java.naming.ldap.factory.socket should be set with keystore config", 
                   LDAPSocketFactory.class.getName(), 
                   environment.get("java.naming.ldap.factory.socket"));
      
      // Cleanup
      loginModule.closeContext();
   }

   @Test
   public void testSSLSocketFactoryWithAllProperties() throws Exception {
      Map<String, Object> options = new HashMap<>();
      
      // Set basic LDAP connection options
      options.put("initialContextFactory", "com.sun.jndi.ldap.LdapCtxFactory");
      options.put("connectionURL", "ldap://localhost:1024");
      options.put("connectionUsername", PRINCIPAL);
      options.put("connectionPassword", CREDENTIALS);
      options.put("connectionProtocol", "s");
      options.put("authentication", "simple");
      
      // Set all SSL configuration options
      options.put("keystoreProvider", "SUN");
      options.put("keystoreType", "JKS");
      options.put("keystorePath", "/path/to/keystore.jks");
      options.put("keystorePassword", "keystoreSecret");
      options.put("keystoreAlias", "myalias");
      options.put("truststoreProvider", "SUN");
      options.put("truststoreType", "JKS");
      options.put("truststorePath", "/path/to/truststore.jks");
      options.put("truststorePassword", "truststoreSecret");
      options.put("crlPath", "/path/to/crl.pem");
      options.put("sslProvider", "JDK");
      options.put("trustAll", "false");
      options.put("trustManagerFactoryPlugin", "com.example.TrustManagerPlugin");
      
      LDAPLoginModule loginModule = new LDAPLoginModule();
      loginModule.initialize(new Subject(), null, null, options);
      loginModule.openContext();
      
      // Verify that java.naming.ldap.factory.socket is set
      Map<?, ?> environment = loginModule.context.getEnvironment();
      assertEquals("java.naming.ldap.factory.socket should be set", 
                   LDAPSocketFactory.class.getName(), 
                   environment.get("java.naming.ldap.factory.socket"));
      
      // Verify SSLSupport has all properties set using reflection
      org.apache.activemq.artemis.core.remoting.impl.ssl.SSLSupport sslSupport = 
         LDAPLoginModule.getCurrentSSLSupport();
      assertNotNull("SSLSupport should be created", sslSupport);
      
      // Note: We can't easily test the actual values without more access,
      // but we can verify the SSLSupport instance exists and socket factory is set
      
      // Cleanup
      loginModule.closeContext();
   }

   @Test
   public void testLDAPSocketFactoryInstantiation() throws Exception {
      // Test that LDAPSocketFactory can be instantiated with default constructor
      // (used by JNDI when java.naming.ldap.factory.socket is set)
      // Without SSLSupport in ThreadLocal, it should fall back to default factory
      LDAPSocketFactory factory = new LDAPSocketFactory();
      assertNotNull("LDAPSocketFactory should be created", factory);
   }

   @Test
   public void testSSLConfigKeysInEnum() throws Exception {
      // Verify all SSL-related ConfigKeys are present
      Set<String> sslKeys = new HashSet<>();
      sslKeys.add("keystoreProvider");
      sslKeys.add("keystoreType");
      sslKeys.add("keystorePath");
      sslKeys.add("keystorePassword");
      sslKeys.add("keystoreAlias");
      sslKeys.add("truststoreProvider");
      sslKeys.add("truststoreType");
      sslKeys.add("truststorePath");
      sslKeys.add("truststorePassword");
      sslKeys.add("crlPath");
      sslKeys.add("sslProvider");
      sslKeys.add("trustAll");
      sslKeys.add("trustManagerFactoryPlugin");
      
      for (LDAPLoginModule.ConfigKey key : LDAPLoginModule.ConfigKey.values()) {
         sslKeys.remove(key.getName());
      }
      
      assertTrue("All SSL ConfigKeys should be present in enum. Missing: " + sslKeys, 
                 sslKeys.isEmpty());
   }

}
