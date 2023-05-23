/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.tests.smoke.console;

import com.github.dockerjava.zerodep.shaded.org.apache.hc.core5.ssl.SSLContexts;
import org.apache.activemq.artemis.tests.smoke.common.SmokeTestBase;
import org.apache.activemq.artemis.util.ServerUtil;
import org.apache.activemq.artemis.utils.RetryRule;
import org.apache.activemq.artemis.utils.Wait;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import javax.net.ssl.SSLContext;
import java.io.File;

public class ConsoleMutualSSLTest extends SmokeTestBase {

   @Rule
   public RetryRule retryRule = new RetryRule(2);

   protected static final String SERVER_NAME = "console-mutual-ssl";
   protected static final String SERVER_ADMIN_USERNAME = "admin";
   protected static final String SERVER_ADMIN_PASSWORD = "admin";

   @Before
   public void before() throws Exception {
      cleanupData(SERVER_NAME);
      disableCheckThread();
      startServer(SERVER_NAME, 0, 0);
      ServerUtil.waitForServerToStart(0, SERVER_ADMIN_USERNAME, SERVER_ADMIN_PASSWORD, 30000);
   }

   @Test
   public void testLoginWithValidCertificate() throws Exception {
      File keyStoreFile = new File(this.getClass().getClassLoader().getResource("client-keystore.p12").getFile());
      File trustStoreFile = new File(this.getClass().getClassLoader().getResource("server-ca-truststore.p12").getFile());
      SSLContext sslContext = SSLContexts.custom()
         .loadKeyMaterial(keyStoreFile, "securepass".toCharArray(), "securepass".toCharArray())
         .loadTrustMaterial(trustStoreFile, "securepass".toCharArray())
         .build();
      try (CloseableHttpClient httpClient = HttpClients.custom().disableRedirectHandling().setSSLContext(sslContext).build()) {
         Wait.assertTrue(() -> {
            try {
               try (CloseableHttpResponse response = httpClient.execute(new HttpGet("https://localhost:8443/console/"))) {
                  return response.getStatusLine().getStatusCode() == 200;
               }
            } catch (Exception ignore) {
               return false;
            }
         }, 5000);
      }
   }

   @Test
   public void testLoginWithInvalidCertificate() throws Exception {
      File keyStoreFile = new File(this.getClass().getClassLoader().getResource("other-client-keystore.p12").getFile());
      File trustStoreFile = new File(this.getClass().getClassLoader().getResource("server-ca-truststore.p12").getFile());
      SSLContext sslContext = SSLContexts.custom()
         .loadKeyMaterial(keyStoreFile, "securepass".toCharArray(), "securepass".toCharArray())
         .loadTrustMaterial(trustStoreFile, "securepass".toCharArray())
         .build();
      try (CloseableHttpClient httpClient = HttpClients.custom().disableRedirectHandling().setSSLContext(sslContext).build()) {
         Wait.assertTrue(() -> {
            try {
               try (CloseableHttpResponse response = httpClient.execute(new HttpGet("https://localhost:8443/console/"))) {
                  return response.getStatusLine().getStatusCode() == 302 &&
                     response.getFirstHeader("Location").getValue().endsWith("auth/login");
               }
            } catch (Exception ignore) {
               return false;
            }
         }, 5000);
      }
   }
}
