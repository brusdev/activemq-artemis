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
package org.apache.activemq.artemis.core.protocol.core;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.concurrent.Executors;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.federation.FederationDownstreamConfiguration;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.protocol.core.impl.ChannelImpl.CHANNEL_ID;
import org.apache.activemq.artemis.core.protocol.core.impl.CoreProtocolManager;
import org.apache.activemq.artemis.core.protocol.core.impl.CoreProtocolManagerFactory;
import org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.FederationDownstreamConnectMessage;
import org.apache.activemq.artemis.core.security.SecurityStore;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.cluster.ClusterManager;
import org.apache.activemq.artemis.core.server.federation.FederationManager;
import org.apache.activemq.artemis.spi.core.protocol.ConnectionEntry;
import org.apache.activemq.artemis.spi.core.remoting.Acceptor;
import org.apache.activemq.artemis.spi.core.remoting.Connection;
import org.apache.activemq.artemis.utils.actors.ArtemisExecutor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the federation downstream authentication guard in
 * {@link CoreProtocolManager} (the private {@code FederationChannelHandler} inner class).
 * <p>
 * The fix replaced the unreliable {@code rc.getSubject() == null} check with
 * {@code !rc.isAuthenticated()} so that security managers that do not populate a
 * {@link javax.security.auth.Subject} (v1–v4) are handled correctly.
 */
public class FederationChannelHandlerTest {

   private ActiveMQServer server;
   private SecurityStore securityStore;
   private FederationManager federationManager;
   private Configuration configuration;

   @BeforeEach
   public void setUp() {
      StorageManager storageManager = mock(StorageManager.class);
      configuration = mock(Configuration.class);
      when(configuration.getConnectionTTLOverride()).thenReturn(-1L);

      securityStore = mock(SecurityStore.class);
      federationManager = mock(FederationManager.class);
      ClusterManager clusterManager = mock(ClusterManager.class);

      server = mock(ActiveMQServer.class);
      when(server.getStorageManager()).thenReturn(storageManager);
      when(server.getConfiguration()).thenReturn(configuration);
      when(server.getNodeID()).thenReturn(SimpleString.of("test-node"));
      when(server.getExecutorFactory()).thenReturn(
         () -> ArtemisExecutor.delegate(Executors.newSingleThreadExecutor()));
      when(server.getSecurityStore()).thenReturn(securityStore);
      when(server.getFederationManager()).thenReturn(federationManager);
      when(server.getClusterManager()).thenReturn(clusterManager);
   }

   /**
    * Builds a {@link CoreProtocolManager}, calls {@link CoreProtocolManager#createConnectionEntry}
    * and returns the {@link ConnectionEntry} so tests can manipulate the RC before invoking
    * the federation handler.
    */
   private ConnectionEntry buildEntry(Connection transport) {
      CoreProtocolManagerFactory factory = mock(CoreProtocolManagerFactory.class);
      CoreProtocolManager manager = new CoreProtocolManager(
         factory, server, Collections.emptyList(), Collections.emptyList());
      Acceptor acceptor = mock(Acceptor.class);
      when(acceptor.getConfiguration()).thenReturn(Collections.emptyMap());
      return manager.createConnectionEntry(acceptor, transport);
   }

   private Connection newTransport() {
      Connection transport = mock(Connection.class);
      when(transport.getID()).thenReturn("test-transport");
      when(transport.getRemoteAddress()).thenReturn("127.0.0.1");
      return transport;
   }

   // -------------------------------------------------------------------------
   // Test: security disabled — packet is processed without closing transport
   // -------------------------------------------------------------------------

   @Test
   public void testHandlesPacketWhenSecurityDisabled() throws Exception {
      when(securityStore.isSecurityEnabled()).thenReturn(false);

      Connection transport = newTransport();
      ConnectionEntry entry = buildEntry(transport);
      CoreRemotingConnection rc = (CoreRemotingConnection) entry.connection;
      ChannelHandler handler = rc.getChannel(CHANNEL_ID.FEDERATION.id, -1).getHandler();

      Packet packet = mock(Packet.class);
      when(packet.getType()).thenReturn(PacketImpl.FEDERATION_DOWNSTREAM_CONNECT);

      // When security is disabled the handler must not close the transport
      // (it will NPE later when extracting config, but transport must survive the auth check)
      try {
         handler.handlePacket(packet);
      } catch (Exception ignored) {
         // NPE expected from the handler trying to process config — that is irrelevant here
      }

      verify(transport, never()).close();
   }

   // -------------------------------------------------------------------------
   // Test: security enabled, connection NOT authenticated → rejected
   // -------------------------------------------------------------------------

   @Test
   public void testRejectsUnauthenticatedConnection() throws Exception {
      when(securityStore.isSecurityEnabled()).thenReturn(true);

      Connection transport = newTransport();
      ConnectionEntry entry = buildEntry(transport);
      CoreRemotingConnection rc = (CoreRemotingConnection) entry.connection;
      // rc.isAuthenticated() == false by default — not authenticated

      ChannelHandler handler = rc.getChannel(CHANNEL_ID.FEDERATION.id, -1).getHandler();

      Packet packet = mock(Packet.class);
      when(packet.getType()).thenReturn(PacketImpl.FEDERATION_DOWNSTREAM_CONNECT);

      handler.handlePacket(packet);

      // The handler must close the transport when the connection is not authenticated
      verify(transport).close();
      // FederationManager must NOT be asked to authorize or deploy anything
      verify(federationManager, never()).authorizeDownstreamDeployment(any());
      verify(federationManager, never()).deploy(any());
   }

   // -------------------------------------------------------------------------
   // Regression test: authenticated WITHOUT a Subject must NOT be rejected.
   //
   // Security managers v1–v4 call setAuthenticated() but never setSubject(), so
   // rc.getSubject() remains null even after a successful login.  The old guard
   // "rc.getSubject() == null" would have wrongly closed such a connection.
   // This test would FAIL if the guard were reverted to rc.getSubject() == null.
   // -------------------------------------------------------------------------

   @Test
   public void testAuthenticatedWithNullSubjectIsNotRejected() throws Exception {
      when(securityStore.isSecurityEnabled()).thenReturn(true);
      when(federationManager.authorizeDownstreamDeployment(null)).thenReturn(true);
      when(configuration.addConnectorConfiguration(
         any(String.class), any(TransportConfiguration.class))).thenReturn(configuration);

      Connection transport = newTransport();
      ConnectionEntry entry = buildEntry(transport);
      CoreRemotingConnection rc = (CoreRemotingConnection) entry.connection;

      // Simulate a v1–v4 security manager: authenticated but no Subject set
      rc.setAuthenticated();
      // rc.getSubject() is null here — that is the exact scenario that the old check got wrong

      ChannelHandler handler = rc.getChannel(CHANNEL_ID.FEDERATION.id, -1).getHandler();

      FederationDownstreamConnectMessage packet = mock(FederationDownstreamConnectMessage.class);
      when(packet.getType()).thenReturn(PacketImpl.FEDERATION_DOWNSTREAM_CONNECT);
      when(packet.getName()).thenReturn("fed1");
      when(packet.getCredentials()).thenReturn(null);
      when(packet.getFederationPolicyMap()).thenReturn(Collections.emptyMap());
      when(packet.getTransformerConfigurationMap()).thenReturn(Collections.emptyMap());
      when(packet.getStreamConfiguration()).thenReturn(new FederationDownstreamConfiguration());

      try {
         handler.handlePacket(packet);
      } catch (Exception ignored) {
         // NPE from missing transport config is acceptable; the key assertion follows.
      }

      // With the OLD guard (rc.getSubject() == null), this transport would have been closed.
      // With the NEW guard (!rc.isAuthenticated()), it must NOT be closed.
      verify(transport, never()).close();
   }

   // -------------------------------------------------------------------------
   // Test: security enabled, authenticated but NOT authorized → rejected
   // -------------------------------------------------------------------------

   @Test
   public void testRejectsUnauthorizedConnection() throws Exception {
      when(securityStore.isSecurityEnabled()).thenReturn(true);
      when(federationManager.authorizeDownstreamDeployment(any())).thenReturn(false);

      Connection transport = newTransport();
      ConnectionEntry entry = buildEntry(transport);
      CoreRemotingConnection rc = (CoreRemotingConnection) entry.connection;
      // Simulate SecurityStoreImpl calling setAuthenticated() after successful authentication
      rc.setAuthenticated();

      ChannelHandler handler = rc.getChannel(CHANNEL_ID.FEDERATION.id, -1).getHandler();

      Packet packet = mock(Packet.class);
      when(packet.getType()).thenReturn(PacketImpl.FEDERATION_DOWNSTREAM_CONNECT);

      handler.handlePacket(packet);

      // Connection must be closed because authorization failed
      verify(transport).close();
      // Federation deploy must not be called
      verify(federationManager, never()).deploy(any());
   }

   // -------------------------------------------------------------------------
   // Test: security enabled, authenticated AND authorized → auth checks pass
   // -------------------------------------------------------------------------

   @Test
   public void testAuthenticatedAndAuthorizedPassesAuthChecks() throws Exception {
      when(securityStore.isSecurityEnabled()).thenReturn(true);
      when(federationManager.authorizeDownstreamDeployment(any())).thenReturn(true);
      // Let addConnectorConfiguration return the config mock to avoid NPE
      when(configuration.addConnectorConfiguration(
         any(String.class), any(TransportConfiguration.class))).thenReturn(configuration);

      Connection transport = newTransport();
      ConnectionEntry entry = buildEntry(transport);
      CoreRemotingConnection rc = (CoreRemotingConnection) entry.connection;
      // Simulate SecurityStoreImpl calling setAuthenticated() after successful authentication
      rc.setAuthenticated();

      ChannelHandler handler = rc.getChannel(CHANNEL_ID.FEDERATION.id, -1).getHandler();

      // Build a minimal real FederationDownstreamConnectMessage
      FederationDownstreamConnectMessage packet = mock(FederationDownstreamConnectMessage.class);
      when(packet.getType()).thenReturn(PacketImpl.FEDERATION_DOWNSTREAM_CONNECT);
      when(packet.getName()).thenReturn("fed1");
      when(packet.getCredentials()).thenReturn(null);
      when(packet.getFederationPolicyMap()).thenReturn(Collections.emptyMap());
      when(packet.getTransformerConfigurationMap()).thenReturn(Collections.emptyMap());

      FederationDownstreamConfiguration downstream = new FederationDownstreamConfiguration();
      when(packet.getStreamConfiguration()).thenReturn(downstream);

      try {
         handler.handlePacket(packet);
      } catch (Exception ignored) {
         // NPE is acceptable if the downstream config lacks a transport configuration;
         // the key assertion is that the transport was NOT closed by the auth check.
      }

      // The transport must NOT be closed by the auth/authz checks
      verify(transport, never()).close();
   }
}
