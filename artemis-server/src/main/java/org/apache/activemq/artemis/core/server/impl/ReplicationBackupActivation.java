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
package org.apache.activemq.artemis.core.server.impl;

import javax.annotation.concurrent.GuardedBy;

import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQIllegalStateException;
import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.protocol.core.Channel;
import org.apache.activemq.artemis.core.replication.ReplicationEndpoint;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.LiveNodeLocator;
import org.apache.activemq.artemis.core.server.NodeManager;;
import org.apache.activemq.artemis.core.server.cluster.ClusterControl;
import org.apache.activemq.artemis.core.server.cluster.ClusterController;
import org.apache.activemq.artemis.core.server.cluster.ha.ReplicationBackupPolicy;
import org.apache.activemq.artemis.quorum.DistributedLock;
import org.apache.activemq.artemis.quorum.DistributedPrimitiveManager;
import org.jboss.logging.Logger;

import static org.apache.activemq.artemis.core.server.impl.ReplicationObserver.ReplicationFailure;

/**
 * This activation can be used by a primary while trying to fail-back ie {@code failback == true} or
 * by a natural-born backup ie {@code failback == false}.<br>
 */
public final class ReplicationBackupActivation extends Activation {

   private static final Logger logger = Logger.getLogger(ReplicationBackupActivation.class);

   private final ReplicationBackupPolicy policy;
   private final ActiveMQServerImpl activeMQServer;
   private final boolean failback;
   // This field is != null iff this node is a primary during a fail-back ie acting as a backup in order to become live again.
   private final String expectedNodeID;
   @GuardedBy("this")
   private boolean closed;
   private final DistributedPrimitiveManager distributedManager;
   // Used for monitoring purposes
   private volatile ReplicationObserver replicationObserver;

   public ReplicationBackupActivation(final ActiveMQServerImpl activeMQServer,
                                      final boolean failback,
                                      final DistributedPrimitiveManager distributedManager,
                                      final ReplicationBackupPolicy policy) {
      this.activeMQServer = activeMQServer;
      this.failback = failback;
      if (failback) {
         final SimpleString serverNodeID = activeMQServer.getNodeID();
         if (serverNodeID == null || serverNodeID.isEmpty()) {
            throw new IllegalStateException("A failback activation must be biased around a specific NodeID");
         }
         this.expectedNodeID = serverNodeID.toString();
      } else {
         this.expectedNodeID = null;
      }
      this.distributedManager = distributedManager;
      this.policy = policy;
      this.replicationObserver = null;
   }

   @Override
   public void run() {
      synchronized (this) {
         if (closed) {
            return;
         }
      }
      try {
         // TODO add a timeout here?
         distributedManager.start().get();
         // Stop the previous node manager and create a new one with NodeManager::replicatedBackup == true:
         // NodeManager::start skip setup lock file with NodeID, until NodeManager::stopBackup is called.
         activeMQServer.resetNodeManager();
         activeMQServer.getNodeManager().stop();
         // A primary need to preserve NodeID across runs
         activeMQServer.moveServerData(policy.getMaxSavedReplicatedJournalsSize(), failback);
         activeMQServer.getNodeManager().start();
         if (!activeMQServer.initialisePart1(false)) {
            return;
         }
         synchronized (this) {
            if (closed)
               return;
         }
         final ClusterController clusterController = activeMQServer.getClusterManager().getClusterController();
         clusterController.awaitConnectionToReplicationCluster();
         activeMQServer.getBackupManager().start();
         ActiveMQServerLogger.LOGGER.backupServerStarted(activeMQServer.getVersion().getFullVersion(),
                                                         activeMQServer.getNodeManager().getNodeId());
         activeMQServer.setState(ActiveMQServerImpl.SERVER_STATE.STARTED);
         final DistributedLock liveLock = replicateAndFailover(clusterController);
         if (liveLock == null) {
            return;
         }
         startAsLive(liveLock);
      } catch (Exception e) {
         if ((e instanceof InterruptedException || e instanceof IllegalStateException) && !activeMQServer.isStarted()) {
            // do not log these errors if the server is being stopped.
            return;
         }
         ActiveMQServerLogger.LOGGER.initializationError(e);
      }
   }

   private void startAsLive(final DistributedLock liveLock) throws Exception {
      assert liveLock.isHeldByCaller();
      policy.getLivePolicy().setBackupPolicy(policy);
      activeMQServer.setHAPolicy(policy.getLivePolicy());

      synchronized (activeMQServer) {
         if (!activeMQServer.isStarted()) {
            liveLock.close();
            return;
         }
         ActiveMQServerLogger.LOGGER.becomingLive(activeMQServer);
         // stopBackup is going to write the NodeID previously set on the NodeManager,
         // because activeMQServer.resetNodeManager() has created a NodeManager with replicatedBackup == true.
         activeMQServer.getNodeManager().stopBackup();
         activeMQServer.getStorageManager().start();
         activeMQServer.getBackupManager().activated();
         // IMPORTANT:
         // we're setting this activation JUST because it would allow the server to use its
         // getActivationChannelHandler to handle replication
         final ReplicationPrimaryActivation primaryActivation = new ReplicationPrimaryActivation(activeMQServer, distributedManager, policy.getLivePolicy());
         liveLock.addListener(primaryActivation);
         activeMQServer.setActivation(primaryActivation);
         activeMQServer.initialisePart2(false);
         if (!liveLock.isHeldByCaller()) {
            throw new ActiveMQIllegalStateException("This server is not live anymore: activation is failed");
         }
         if (activeMQServer.getIdentity() != null) {
            ActiveMQServerLogger.LOGGER.serverIsLive(activeMQServer.getIdentity());
         } else {
            ActiveMQServerLogger.LOGGER.serverIsLive();
         }
         activeMQServer.completeActivation(true);
      }
   }

   private LiveNodeLocator createLiveNodeLocator(final LiveNodeLocator.BackupRegistrationListener registrationListener) {
      if (expectedNodeID != null) {
         assert failback;
         return new NamedLiveNodeIdLocatorForReplication(expectedNodeID, registrationListener, policy.getRetryReplicationWait());
      }
      return policy.getGroupName() == null ?
         new AnyLiveNodeLocatorForReplication(registrationListener, activeMQServer, policy.getRetryReplicationWait()) :
         new NamedLiveNodeLocatorForReplication(policy.getGroupName(), registrationListener, policy.getRetryReplicationWait());
   }

   private DistributedLock replicateAndFailover(final ClusterController clusterController) throws ActiveMQException, InterruptedException {
      while (true) {
         synchronized (this) {
            if (closed) {
               return null;
            }
         }
         final ReplicationFailure failure = replicateLive(clusterController);
         if (failure == null) {
            continue;
         }
         if (!activeMQServer.isStarted()) {
            return null;
         }
         switch (failure) {
            case VoluntaryFailOver:
            case NonVoluntaryFailover:
               final DistributedLock liveLock = tryAcquireLiveLock();
               if (liveLock != null) {
                  return liveLock;
               }
               if (!failback) {
                  ActiveMQServerLogger.LOGGER.restartingAsBackupBasedOnQuorumVoteResults();
               }
               asyncRestartServer(activeMQServer);
               return null;
            case RegistrationError:
               // can just retry here, data should be clean
               continue;
            case AlreadyReplicating:
               // can just retry here, data should be clean
               continue;
            case ClosedObserver:
               return null;
            case BackupNotInSync:
               asyncRestartServer(activeMQServer);
               return null;
            case WrongNodeId:
               asyncRestartServer(activeMQServer);
               return null;
            default:
               throw new AssertionError("Unsupported failure " + failure);
         }
      }
   }

   private DistributedLock tryAcquireLiveLock() throws InterruptedException {
      assert activeMQServer.getNodeManager().getNodeId() != null;
      final String liveID = activeMQServer.getNodeManager().getNodeId().toString();
      final DistributedLock liveLock = getLock(distributedManager, liveID);
      if (liveLock == null) {
         return null;
      }
      final int voteRetries = policy.getVoteRetries();
      final long voteRetryWait = policy.getVoteRetryWait();
      for (int retries = 0; retries < voteRetries; retries++) {
         try {
            if (liveLock.tryLock()) {
               return liveLock;
            }
         } catch (Throwable t) {
            logger.warnf(t, "Failed to acquire live lock %s", liveID);
         }
         TimeUnit.MILLISECONDS.sleep(voteRetryWait);
      }
      return null;
   }

   private DistributedLock getLock(final DistributedPrimitiveManager manager, final String lockId) {
      try {
         return manager.getDistributedLock(lockId);
      } catch (Throwable e) {
         logger.warnf(e, "Failed to get live lock %s", lockId);
         return null;
      }
   }

   private ReplicationObserver replicationObserver() {
      if (failback) {
         return ReplicationObserver.failbackObserver(activeMQServer.getNodeManager(), activeMQServer.getBackupManager(), activeMQServer.getScheduledPool(), expectedNodeID);
      }
      return ReplicationObserver.failoverObserver(activeMQServer.getNodeManager(), activeMQServer.getBackupManager(), activeMQServer.getScheduledPool());
   }

   private ReplicationFailure replicateLive(final ClusterController clusterController) throws ActiveMQException, InterruptedException {
      try (ReplicationObserver replicationObserver = replicationObserver()) {
         this.replicationObserver = replicationObserver;
         final LiveNodeLocator liveLocator = createLiveNodeLocator(replicationObserver);
         clusterController.addClusterTopologyListener(replicationObserver);
         clusterController.addClusterTopologyListenerForReplication(liveLocator);
         // ReplicationError notifies observer of backup registration failures trough node locator
         final ReplicationError replicationError = new ReplicationError(liveLocator);
         clusterController.addIncomingInterceptorForReplication(replicationError);
         try (ClusterControl liveControl = tryLocateAndConnectToLive(liveLocator, clusterController)) {
            if (liveControl == null) {
               Thread.sleep(clusterController.getRetryIntervalForReplicatedCluster());
               return null;
            }
            final ReplicationEndpoint replicationEndpoint = tryAuthorizeAndAsyncRegisterAsBackupToLive(liveControl, replicationObserver);
            if (replicationEndpoint == null) {
               Thread.sleep(clusterController.getRetryIntervalForReplicatedCluster());
               return null;
            }
            assert replicationEndpoint != null;
            try {
               final ReplicationFailure failure = replicationObserver.awaitReplicationFailure();
               assert validateLiveFailure(replicationObserver, failure);
               return failure;
            } finally {
               ActiveMQServerImpl.stopComponent(replicationEndpoint);
               closeChannelOf(replicationEndpoint);
            }
         } finally {
             this.replicationObserver = null;
            clusterController.removeClusterTopologyListener(replicationObserver);
            clusterController.removeClusterTopologyListenerForReplication(liveLocator);
            clusterController.removeIncomingInterceptorForReplication(replicationError);
         }
      }
   }

   private boolean validateLiveFailure(final ReplicationObserver liveObserver,
                                       final ReplicationFailure failure) {
      switch (failure) {
         case NonVoluntaryFailover:
         case VoluntaryFailOver:
            final SimpleString nodeId = activeMQServer.getNodeManager().getNodeId();
            if (nodeId == null) {
               return false;
            }
            if (!nodeId.toString().equals(liveObserver.getLiveID())) {
               return false;
            }
            if (liveObserver.isBackupUpToDate()) {
               return false;
            }
      }
      return true;
   }

   private static void closeChannelOf(final ReplicationEndpoint replicationEndpoint) {
      if (replicationEndpoint == null) {
         return;
      }
      if (replicationEndpoint.getChannel() != null) {
         replicationEndpoint.getChannel().close();
         replicationEndpoint.setChannel(null);
      }
   }

   private void asyncRestartServer(final ActiveMQServer server) {
      new Thread(() -> {
         try {
            if (server.getState() != ActiveMQServer.SERVER_STATE.STOPPED && server.getState() != ActiveMQServer.SERVER_STATE.STOPPING) {
               server.stop(false);
               if (failback) {
                  policy.getLivePolicy().setBackupPolicy(policy);
                  activeMQServer.setHAPolicy(policy.getLivePolicy());
                  // don't use the existing data, because are garbage: clean start
                  activeMQServer.moveServerData(policy.getMaxSavedReplicatedJournalsSize(), true);
               }
               server.start();
            }
         } catch (Exception e) {
            ActiveMQServerLogger.LOGGER.errorRestartingBackupServer(e, server);
         }
      }).start();
   }

   private ClusterControl tryLocateAndConnectToLive(final LiveNodeLocator liveLocator,
                                                    final ClusterController clusterController) throws ActiveMQException {
      liveLocator.locateNode();
      final Pair<TransportConfiguration, TransportConfiguration> possibleLive = liveLocator.getLiveConfiguration();
      final String nodeID = liveLocator.getNodeID();
      if (nodeID == null) {
         throw new RuntimeException("Could not establish the connection with any live");
      }
      if (!failback) {
         assert expectedNodeID == null;
         activeMQServer.getNodeManager().setNodeID(nodeID);
      } else {
         assert expectedNodeID.equals(nodeID);
      }
      if (possibleLive == null) {
         return null;
      }
      final ClusterControl liveControl = tryConnectToNodeInReplicatedCluster(clusterController, possibleLive.getA());
      if (liveControl != null) {
         return liveControl;
      }
      return tryConnectToNodeInReplicatedCluster(clusterController, possibleLive.getB());
   }

   private static ClusterControl tryConnectToNodeInReplicatedCluster(final ClusterController clusterController,
                                                                     final TransportConfiguration tc) {
      try {
         if (tc != null) {
            return clusterController.connectToNodeInReplicatedCluster(tc);
         }
      } catch (Exception e) {
         logger.debug(e.getMessage(), e);
      }
      return null;
   }

   @Override
   public void close(final boolean permanently, final boolean restarting) throws Exception {
      synchronized (this) {
         closed = true;
         final ReplicationObserver replicationObserver = this.replicationObserver;
         if (replicationObserver != null) {
            replicationObserver.close();
         }
      }
      distributedManager.stop().get();
      //we have to check as the server policy may have changed
      if (activeMQServer.getHAPolicy().isBackup()) {
         // To avoid a NPE cause by the stop
         final NodeManager nodeManager = activeMQServer.getNodeManager();

         activeMQServer.interruptActivationThread(nodeManager);

         if (nodeManager != null) {
            nodeManager.stopBackup();
         }
      }
   }

   @Override
   public void preStorageClose() throws Exception {
      // TODO replication endpoint close?
   }

   private ReplicationEndpoint tryAuthorizeAndAsyncRegisterAsBackupToLive(final ClusterControl liveControl,
                                                                          final ReplicationObserver liveObserver) {
      ReplicationEndpoint replicationEndpoint = null;
      try {
         liveControl.getSessionFactory().setReconnectAttempts(1);
         liveObserver.listenConnectionFailuresOf(liveControl.getSessionFactory());
         liveControl.authorize();
         replicationEndpoint = new ReplicationEndpoint(activeMQServer, failback, liveObserver);
         replicationEndpoint.setExecutor(activeMQServer.getExecutorFactory().getExecutor());
         connectToReplicationEndpoint(liveControl, replicationEndpoint);
         replicationEndpoint.start();
         liveControl.announceReplicatingBackupToLive(failback, policy.getClusterName());
         return replicationEndpoint;
      } catch (Exception e) {
         ActiveMQServerLogger.LOGGER.replicationStartProblem(e);
         ActiveMQServerImpl.stopComponent(replicationEndpoint);
         closeChannelOf(replicationEndpoint);
         return null;
      }
   }

   private boolean connectToReplicationEndpoint(final ClusterControl liveControl,
                                                final ReplicationEndpoint replicationEndpoint) {
      final Channel replicationChannel = liveControl.createReplicationChannel();
      replicationChannel.setHandler(replicationEndpoint);
      replicationEndpoint.setChannel(replicationChannel);
      return true;
   }

   @Override
   public boolean isReplicaSync() {
      // NOTE: this method is just for monitoring purposes, not suitable to perform logic!
      // During a failover this backup won't have any active liveObserver and will report `false`!!
      final ReplicationObserver liveObserver = this.replicationObserver;
      if (liveObserver == null) {
         return false;
      }
      return liveObserver.isBackupUpToDate();
   }
}
