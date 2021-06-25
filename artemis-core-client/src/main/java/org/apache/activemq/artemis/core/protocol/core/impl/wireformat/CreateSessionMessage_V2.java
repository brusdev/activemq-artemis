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
package org.apache.activemq.artemis.core.protocol.core.impl.wireformat;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;

import java.util.HashMap;
import java.util.Map;

public class CreateSessionMessage_V2 extends CreateSessionMessage {

   private Map<String, String> metadata = new HashMap<>();

   public CreateSessionMessage_V2(final String name,
                               final long sessionChannelID,
                               final int version,
                               final String username,
                               final String password,
                               final int minLargeMessageSize,
                               final boolean xa,
                               final boolean autoCommitSends,
                               final boolean autoCommitAcks,
                               final boolean preAcknowledge,
                               final int windowSize,
                               final String defaultAddress,
                               final Map<String, String> metadata) {
      super(CREATESESSION_V2, name, sessionChannelID, version, username, password, minLargeMessageSize, xa, autoCommitSends, autoCommitAcks, preAcknowledge, windowSize, defaultAddress);

      this.metadata = metadata;
   }

   public CreateSessionMessage_V2() {
      super(CREATESESSION_V2);
   }

   // Public --------------------------------------------------------


   public Map<String, String> getMetadata() {
      return metadata;
   }

   @Override
   public void encodeRest(final ActiveMQBuffer buffer) {
      super.encodeRest(buffer);

      buffer.writeInt(metadata == null ? 0 : metadata.size());
      if (metadata != null) {
         for (Map.Entry<String, String> metadataEntry : metadata.entrySet()) {
            buffer.writeString(metadataEntry.getKey());
            buffer.writeNullableString(metadataEntry.getValue());
         }
      }
   }

   @Override
   public void decodeRest(final ActiveMQBuffer buffer) {
      super.decodeRest(buffer);

      int metadataSize = buffer.readInt();
      for (int i = 0; i < metadataSize; i++) {
         String key = buffer.readString();
         String value = buffer.readNullableString();
         metadata.put(key, value);
      }
   }

   @Override
   public String toString() {
      StringBuffer buf = new StringBuffer(getParentString());
      buf.append(", metadata=" + metadata);
      buf.append("]");
      return buf.toString();
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + ((metadata == null) ? 0 : metadata.hashCode());
      return result;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj) {
         return true;
      }
      if (!super.equals(obj)) {
         return false;
      }
      if (!(obj instanceof CreateSessionMessage_V2)) {
         return false;
      }
      CreateSessionMessage_V2 other = (CreateSessionMessage_V2) obj;
      if (metadata == null) {
         if (other.metadata != null)
            return false;
      } else if (!metadata.equals(other.metadata))
         return false;
      return true;
   }
}
