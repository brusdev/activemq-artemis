package org.apache.activemq.artemis.core.server.balancing.pools;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQComponent;

public abstract class DiscoveryService implements ActiveMQComponent {

   private Listener listener;

   public Listener getListener() {
      return listener;
   }

   public void setListener(Listener listener) {
      this.listener = listener;
   }

   protected void fireEntryAddedEvent(String nodeID, TransportConfiguration connector) {
      if (listener != null) {
         this.listener.entryAdded(nodeID, connector);
      }
   }

   protected void fireEntryRemovedEvent(String nodeID, TransportConfiguration connector) {
      if (listener != null) {
         this.listener.entryRemoved(nodeID, connector);
      }
   }

   public interface Listener {
      void entryAdded(String nodeID, TransportConfiguration connector);

      void entryRemoved(String nodeID, TransportConfiguration connector);
   }
}
