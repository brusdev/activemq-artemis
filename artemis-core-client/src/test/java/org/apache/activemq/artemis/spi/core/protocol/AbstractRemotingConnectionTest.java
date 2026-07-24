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
package org.apache.activemq.artemis.spi.core.protocol;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.core.protocol.core.impl.PacketDecoder;
import org.apache.activemq.artemis.core.protocol.core.impl.RemotingConnectionImpl;
import org.apache.activemq.artemis.spi.core.remoting.Connection;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link AbstractRemotingConnection#setAuthenticated()} and
 * {@link AbstractRemotingConnection#isAuthenticated()}.
 */
public class AbstractRemotingConnectionTest {

   /**
    * Creates a minimal {@link AbstractRemotingConnection} (client-side) backed by a mocked transport
    * connection.
    */
   private AbstractRemotingConnection newConnection() {
      Connection transport = Mockito.mock(Connection.class);
      return new AbstractRemotingConnection(transport, null) {
         @Override
         public void fail(ActiveMQException me, String scaleDownTargetNodeID) {

         }

         @Override
         public void destroy() {

         }

         @Override
         public void disconnect(boolean criticalError) {

         }

         @Override
         public void disconnect(String scaleDownNodeID, boolean criticalError) {

         }

         @Override
         public String getProtocolName() {
            return "";
         }
      };
   }

   @Test
   public void testIsAuthenticatedReturnsFalseByDefault() {
      AbstractRemotingConnection conn = newConnection();
      assertFalse(conn.isAuthenticated(),
                  "A newly created connection must not be authenticated");
   }

   @Test
   public void testSetAuthenticatedMarksConnectionAsAuthenticated() {
      AbstractRemotingConnection conn = newConnection();
      conn.setAuthenticated();
      assertTrue(conn.isAuthenticated(),
                 "Connection must be authenticated after setAuthenticated() is called");
   }

   @Test
   public void testSetAuthenticatedIsIdempotent() {
      AbstractRemotingConnection conn = newConnection();
      conn.setAuthenticated();
      conn.setAuthenticated();
      assertTrue(conn.isAuthenticated(),
                 "Calling setAuthenticated() multiple times must still leave the connection authenticated");
   }
}
