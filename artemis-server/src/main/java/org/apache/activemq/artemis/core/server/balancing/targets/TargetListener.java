package org.apache.activemq.artemis.core.server.balancing.targets;

public interface TargetListener {
   void targetConnected();

   void targetDisconnected();
}
