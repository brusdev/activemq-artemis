package org.apache.activemq.artemis.core.server.balancing.targets;

public interface TargetFactory {
   Target createTarget(TargetReference reference);
}
