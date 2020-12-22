/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.distributed.internal.membership.rapid;

import static org.apache.geode.distributed.internal.membership.gms.membership.GMSJoinLeave.BYPASS_DISCOVERY_PROPERTY;
import static org.apache.geode.distributed.internal.membership.gms.membership.GMSJoinLeave.JOIN_RETRY_SLEEP;

import java.io.IOException;
import java.io.NotSerializableException;
import java.net.ConnectException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.net.HostAndPort;
import com.google.protobuf.ByteString;
import com.vrg.rapid.Cluster;
import com.vrg.rapid.NodeStatusChange;
import com.vrg.rapid.messaging.impl.NettyClientServer;
import com.vrg.rapid.pb.EdgeStatus;
import com.vrg.rapid.pb.Endpoint;
import com.vrg.rapid.pb.Metadata;
import org.apache.commons.lang3.SerializationException;
import org.apache.logging.log4j.Logger;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import org.apache.geode.distributed.internal.membership.api.Authenticator;
import org.apache.geode.distributed.internal.membership.api.LifecycleListener;
import org.apache.geode.distributed.internal.membership.api.MemberDisconnectedException;
import org.apache.geode.distributed.internal.membership.api.MemberIdentifier;
import org.apache.geode.distributed.internal.membership.api.MemberIdentifierFactory;
import org.apache.geode.distributed.internal.membership.api.MemberShunnedException;
import org.apache.geode.distributed.internal.membership.api.MemberStartupException;
import org.apache.geode.distributed.internal.membership.api.Membership;
import org.apache.geode.distributed.internal.membership.api.MembershipClosedException;
import org.apache.geode.distributed.internal.membership.api.MembershipConfig;
import org.apache.geode.distributed.internal.membership.api.MembershipConfigurationException;
import org.apache.geode.distributed.internal.membership.api.MembershipListener;
import org.apache.geode.distributed.internal.membership.api.MembershipLocator;
import org.apache.geode.distributed.internal.membership.api.MembershipStatistics;
import org.apache.geode.distributed.internal.membership.api.MembershipView;
import org.apache.geode.distributed.internal.membership.api.Message;
import org.apache.geode.distributed.internal.membership.api.MessageListener;
import org.apache.geode.distributed.internal.membership.api.QuorumChecker;
import org.apache.geode.distributed.internal.membership.api.StopShunningMarker;
import org.apache.geode.distributed.internal.membership.gms.GMSMemberData;
import org.apache.geode.distributed.internal.membership.gms.GMSMembership;
import org.apache.geode.distributed.internal.membership.gms.GMSUtil;
import org.apache.geode.distributed.internal.membership.gms.locator.FindCoordinatorRequest;
import org.apache.geode.distributed.internal.membership.gms.locator.FindCoordinatorResponse;
import org.apache.geode.distributed.internal.tcpserver.TcpClient;
import org.apache.geode.distributed.internal.tcpserver.TcpSocketCreator;
import org.apache.geode.internal.inet.LocalHostUtil;
import org.apache.geode.internal.serialization.BufferDataOutputStream;
import org.apache.geode.internal.serialization.ByteArrayDataInput;
import org.apache.geode.internal.serialization.DSFIDSerializer;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.logging.internal.OSProcess;
import org.apache.geode.logging.internal.executors.LoggingExecutors;
import org.apache.geode.logging.internal.executors.LoggingThread;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.util.internal.GeodeGlossary;

public class MembershipImpl<ID extends MemberIdentifier> implements Membership<ID> {
  private static final Logger logger = LogService.getLogger();
  public static final String GEODE_ID = "geodeId";

  private final MembershipStatistics statistics;
  private final Authenticator<ID> authenticator;
  private final MembershipConfig membershipConfig;
  private final DSFIDSerializer serializer;

  /**
   * the address that the messenger is listening on
   */
  private HostAndPort listenAddress;
  /**
   * The Netty messenger used for membership communications
   */
  private NettyClientServer messenger;

  /**
   * registrants during discovery
   */
  private Set<ID> registrants;

  public MemberIdentifierFactory<ID> getMemberFactory() {
    return memberFactory;
  }

  private final MemberIdentifierFactory<ID> memberFactory;
  private final TcpClient locatorClient;
  private final TcpSocketCreator socketCreator;

  private final Stopper cancelCriterion;

  private MembershipListener<ID> membershipListener;
  private MessageListener<ID> messageListener;
  private LifecycleListener<ID> lifecycleListener;

  private RapidSeedLocator<ID> locator;
  private MembershipLocator<ID> membershipLocator;

  private final ExecutorService viewExecutor;
  /**
   * Members of the distributed system that we believe have shut down. Keys are instances of
   * {@link ID}, values are Longs indicating the time this member was
   * shunned.
   *
   * Members are removed after {@link #SHUNNED_SUNSET} seconds have passed.
   *
   * Accesses to this list needs to be under the read or write lock of {@link #latestViewLock}
   *
   * @see System#currentTimeMillis()
   */
  // protected final Set shunnedMembers = Collections.synchronizedSet(new HashSet());
  private final Map<ID, Long> shunnedMembers = new ConcurrentHashMap<>();

  /**
   * Members that have sent a shutdown message. This is used to suppress suspect processing that
   * otherwise becomes pretty aggressive when a member is shutting down.
   */
  private final Map<ID, Object> shutdownMembers = new GMSMembership.BoundedLinkedHashMap<>();

  /**
   * per bug 39552, keep a list of members that have been shunned and for which a message is
   * printed. Contents of this list are cleared at the same time they are removed from
   * {@link #shunnedMembers}.
   *
   * Accesses to this list needs to be under the read or write lock of {@link #latestViewLock}
   */
  private final HashSet<ID> shunnedAndWarnedMembers = new HashSet<>();
  /**
   * The identities and birth-times of others that we have allowed into membership at the
   * distributed system level, but have not yet appeared in a view.
   * <p>
   * Keys are instances of {@link ID}, values are Longs indicating the time
   * this member was shunned.
   * <p>
   * Members are removed when a view containing them is processed. If, after
   * {@link #surpriseMemberTimeout} milliseconds have passed, a view containing the member has not
   * arrived, the member is removed from membership and member-left notification is performed.
   * <p>
   * > Accesses to this list needs to be under the read or write lock of {@link #latestViewLock}
   *
   * @see System#currentTimeMillis()
   */
  private final Map<ID, Long> surpriseMembers = new ConcurrentHashMap<>();

  /**
   * the timeout interval for surprise members. This is calculated from the member-timeout setting
   */
  private long surpriseMemberTimeout;

  /**
   * javagroups can skip views and omit telling us about a crashed member. This map holds a history
   * of suspected members that we use to detect crashes.
   */
  private final Map<ID, Long> suspectedMembers = new ConcurrentHashMap<>();

  /**
   * Length of time, in seconds, that a member is retained in the zombie set
   *
   * @see #shunnedMembers
   */
  private static final int SHUNNED_SUNSET = Integer
      .getInteger(GeodeGlossary.GEMFIRE_PREFIX + "shunned-member-timeout", 300).intValue();


  /**
   * This is the lock for protecting access to latestView
   *
   * @see #latestView
   */
  private final ReadWriteLock latestViewLock = new ReentrantReadWriteLock();
  private final Lock latestViewReadLock = latestViewLock.readLock();
  private final Lock latestViewWriteLock = latestViewLock.writeLock();

  /**
   * This object synchronizes threads waiting for startup to finish. Updates to
   * {@link #startupMessages} are synchronized through this object.
   */
  private final GMSMembership.EventProcessingLock startupLock =
      new GMSMembership.EventProcessingLock();

  /**
   * Special mutex to create a critical section for {@link #startEventProcessing()}
   */
  private final Object startupMutex = new Object();

  /**
   * A list of messages received during channel startup that couldn't be processed yet. Additions or
   * removals of this list must be synchronized via {@link #startupLock}.
   *
   * @since GemFire 5.0
   */
  private final LinkedList<StartupEvent<ID>> startupMessages = new LinkedList<>();


  /**
   * Rapid Cluster object
   */
  private Cluster cluster;
  private Cluster.Builder clusterBuilder;

  private ID localAddress;
  private volatile boolean shutdownInProgress;
  private Exception shutdownCause;

  private volatile boolean beingSick;
  private volatile boolean playingDead;

  volatile boolean isJoining;
  /** have we joined successfully? */
  private volatile boolean hasJoined;

  /**
   * This is the latest view (ordered list of IDs) that has been installed
   *
   * All accesses to this object are protected via {@link #latestViewLock}
   */
  private volatile MembershipView<ID> latestView = new MembershipView<>();

  /**
   * The next view change will establish the first membership view
   */
  private boolean firstView = true;

  /**
   * Set to true when upcalls should be generated for events.
   */
  private volatile boolean processingEvents = false;

  /**
   * Set to true under startupLock when processingEvents has been set to true
   * and startup messages have been removed from the queue and dispatched
   */
  private boolean startupMessagesDrained = false;

  /**
   * a status set by DistributionImpl during shutdown
   */
  private boolean isCloseInProgress;


  public MembershipImpl(
      MembershipStatistics statistics,
      Authenticator<ID> authenticator,
      MembershipConfig membershipConfig,
      DSFIDSerializer serializer, MemberIdentifierFactory<ID> memberFactory,
      TcpClient locatorClient,
      TcpSocketCreator socketCreator) {

    this.statistics = statistics;
    this.authenticator = authenticator;
    this.membershipConfig = membershipConfig;
    this.serializer = serializer;
    this.memberFactory = memberFactory;
    this.locatorClient = locatorClient;
    this.socketCreator = socketCreator;
    this.viewExecutor = LoggingExecutors.newSingleThreadExecutor("Geode View Processor", true);
    this.cancelCriterion = new Stopper();
  }

  public void setLocators(final RapidSeedLocator<ID> locator,
      final MembershipLocator<ID> membershipLocator) {
    this.locator = locator;
    this.membershipLocator = membershipLocator;
  }

  @Override
  public MembershipView getView() {
    return latestView;
  }

  @Override
  public ID getLocalMember() {
    return localAddress;
  }

  @Override
  public Set<ID> send(MemberIdentifier[] destinations, Message content)
      throws NotSerializableException {
    throw new NotImplementedException();
  }

  @Override
  public boolean requestMemberRemoval(ID member, String reason)
      throws MemberDisconnectedException {
    return false;
  }

  @Override
  public boolean verifyMember(ID mbr, String reason) {
    return false;
  }

  @Override
  public boolean isShunned(ID m) {
    if (!shunnedMembers.containsKey(m)) {
      return false;
    }

    latestViewWriteLock.lock();
    try {
      // Make sure that the entry isn't stale...
      long shunTime = shunnedMembers.get(m).longValue();
      long now = System.currentTimeMillis();
      if (shunTime + SHUNNED_SUNSET * 1000L > now) {
        return true;
      }

      // Oh, it _is_ stale. Remove it while we're here.
      endShun(m);
      return false;
    } finally {
      latestViewWriteLock.unlock();
    }
  }

  @Override
  public boolean memberExists(ID m) {
    latestViewReadLock.lock();
    MembershipView<ID> v = latestView;
    latestViewReadLock.unlock();
    return v.contains(m);
  }

  @Override
  public boolean isConnected() {
    return hasJoined;
  }

  @Override
  public void beSick() {
    beingSick = true;
  }

  @Override
  public void playDead() {
    playingDead = true;
  }

  @Override
  public void beHealthy() {
    beingSick = false;
    playingDead = false;
  }

  @Override
  public boolean isBeingSick() {
    return beingSick;
  }

  @Override
  public void disconnect(boolean beforeJoined) {
    if (beforeJoined) {
      uncleanShutdown("Failed to start distribution", null);
    } else {
      shutdown();
    }
  }

  @Override
  public void shutdown() {
    logger.info("Shutting down membership {}", localAddress);
    setShutdown();
    try {
      cluster.leaveGracefully();
      messenger.shutdown();
    } finally {
      try {
        stop();
      } finally {
        viewExecutor.shutdown();
      }
    }
  }

  private void stop() {
    logger.debug("Membership closing");

    if (lifecycleListener.disconnect(null)) {

      if (localAddress != null) {
        // Make sure that channel information is consistent
        // Probably not important in this particular case, but just
        // to be consistent...
        latestViewWriteLock.lock();
        try {
          destroyMember(localAddress, "orderly shutdown");
        } finally {
          latestViewWriteLock.unlock();
        }
      }
    }

    if (logger.isDebugEnabled()) {
      logger.debug("Membership: channel closed");
    }
  }

  public void uncleanShutdown(String reason, Exception e) {
    try {
      // if (cleanupTimer != null && !cleanupTimer.isShutdown()) {
      // cleanupTimer.shutdownNow();
      // }
      if (shutdownCause == null) {
        shutdownCause = e;
      }

      lifecycleListener.disconnect(e);
      cluster.shutdown();

    } finally {
      if (e != null) {
        try {
          membershipListener.membershipFailure(reason, e);
        } catch (RuntimeException re) {
          logger.warn("Exception caught while shutting down", re);
        }
      }
    }
  }

  /**
   * Clean up and create consistent new view with member removed. No uplevel events are generated.
   *
   * Must be called with the {@link #latestViewLock} held.
   */
  private void destroyMember(final ID member, final String reason) {

    // Make sure it is removed from the view
    latestViewWriteLock.lock();
    try {
      if (latestView.contains(member)) {
        MembershipView<ID> newView = new MembershipView<>(latestView, latestView.getViewId());
        newView.remove(member);
        newView.makeUnmodifiable();
        latestView = newView;
      }
    } finally {
      latestViewWriteLock.unlock();
    }

    surpriseMembers.remove(member);

    // Trickiness: there is a minor recursion
    // with addShunnedMembers, since it will
    // attempt to destroy really really old members. Performing the check
    // here breaks the recursion.
    if (!isShunned(member)) {
      addShunnedMember(member);
    }

    lifecycleListener.destroyMember(member, reason);
  }


  @Override
  public void shutdownMessageReceived(ID id, String reason) {
    if (logger.isDebugEnabled()) {
      logger.debug("Membership: recording shutdown status of {}", id);
    }
    synchronized (this.shutdownMembers) {
      this.shutdownMembers.put(id, id);
    }
  }

  @Override
  public void waitForEventProcessing() throws InterruptedException {
    // First check outside of a synchronized block. Cheaper and sufficient.
    if (Thread.interrupted())
      throw new InterruptedException();
    if (processingEvents)
      return;
    if (logger.isDebugEnabled()) {
      logger.debug("Membership: waiting until the system is ready for events");
    }
    for (;;) {
      cancelCriterion.checkCancelInProgress(null);
      synchronized (startupLock) {
        // Now check using a memory fence and synchronization.
        if (processingEvents && startupMessagesDrained) {
          break;
        }
        boolean interrupted = Thread.interrupted();
        try {
          startupLock.wait();
        } catch (InterruptedException e) {
          interrupted = true;
          cancelCriterion.checkCancelInProgress(e);
        } finally {
          if (interrupted) {
            Thread.currentThread().interrupt();
          }
        }
      } // synchronized
    } // for
    if (logger.isDebugEnabled()) {
      logger.debug("Membership: continuing");
    }
  }

  @Override
  public void startEventProcessing() {
    // Only allow one thread to perform the work
    synchronized (startupMutex) {
      if (logger.isDebugEnabled())
        logger.debug("Membership: draining startup events.");
      // Remove the backqueue of messages, but allow
      // additional messages to be added.
      for (;;) {
        StartupEvent<ID> ev;
        // Only grab the mutex while reading the queue.
        // Other events may arrive while we're attempting to
        // drain the queue. This is OK, we'll just keep processing
        // events here until we've caught up.
        synchronized (startupLock) {
          int remaining = startupMessages.size();
          if (remaining == 0) {
            // While holding the lock, flip the bit so that
            // no more events get put into startupMessages
            startupMessagesDrained = true;
            // set the volatile boolean that states that queueing is completely done now
            processingEvents = true;
            // notify any threads waiting for event processing that we're open for business
            startupLock.notifyAll();
            break;
          }
          if (logger.isDebugEnabled()) {
            logger.debug("Membership: {} remaining startup message(s)", remaining);
          }
          ev = startupMessages.removeFirst();
        } // startupLock
        try {
          processStartupEvent(ev);
        } catch (VirtualMachineError err) {
          // If this ever returns, rethrow the error. We're poisoned
          // now, so don't let this thread continue.
          throw err;
        } catch (Throwable t) {
          logger.warn("Membership: Error handling startup event",
              t);
        }
      } // for
      if (logger.isDebugEnabled())
        logger.debug("Membership: finished processing startup events.");
    }
  }

  @Override
  public void setShutdown() {
    shutdownInProgress = true;
  }

  @Override
  public void setReconnectCompleted(boolean reconnectCompleted) {
    // TODO - reconnect not implemented
  }

  @Override
  public boolean shutdownInProgress() {
    return shutdownInProgress;
  }

  @Override
  public boolean waitForNewMember(MemberIdentifier remoteId) {
    // TODO not implemented yet - also need the latch stuff in GMSMembership.processView
    return true;
  }

  @Override
  public void emergencyClose() {
    viewExecutor.shutdownNow();
  }

  @Override
  public void addSurpriseMemberForTesting(ID mbr, long birthTime) {
    if (logger.isDebugEnabled()) {
      logger.debug("test hook is adding surprise member {} birthTime={}", mbr, birthTime);
    }
    latestViewWriteLock.lock();
    try {
      surpriseMembers.put(mbr, birthTime);
    } finally {
      latestViewWriteLock.unlock();
    }
  }

  @Override
  public void suspectMembers(Set members, String reason) {
    // TODO not implemented
  }

  @Override
  public void suspectMember(ID member, String reason) {
    // TODO not implemented
  }

  @Override
  public Throwable getShutdownCause() {
    return shutdownCause;
  }

  @Override
  public void warnShun(ID memberIdentifier) {
    latestViewWriteLock.lock();
    try {
      if (!shunnedMembers.containsKey(memberIdentifier)) {
        return; // not shunned
      }
      if (shunnedAndWarnedMembers.contains(memberIdentifier)) {
        return; // already warned
      }
      shunnedAndWarnedMembers.add(memberIdentifier);
    } finally {
      latestViewWriteLock.unlock();
    }
    // issue warning outside of sync since it may cause messaging and we don't
    // want to hold the view lock while doing that
    logger.warn("Membership: disregarding shunned member <{}>", memberIdentifier);
  }

  @Override
  public boolean addSurpriseMember(ID mbr) {
    final ID member = mbr;
    boolean warn = false;

    latestViewWriteLock.lock();
    try {
      // At this point, the join may have been discovered by
      // other means.
      if (latestView.contains(member)) {
        return true;
      }
      if (surpriseMembers.containsKey(member)) {
        return true;
      }
//      if (member.getVmViewId() < 0) {
//        logger.warn(
//            "adding a surprise member that has not yet joined the distributed system: " + member,
//            new Exception("stack trace"));
//      }
//      if (latestView.getViewId() > member.getVmViewId()) {
//        // tell the process that it should shut down distribution.
//        // Run in a separate thread so we don't hold the view lock during the request. Bug #44995
//        new LoggingThread("Removing shunned GemFire node " + member, false, () -> {
//          // fix for bug #42548
//          // this is an old member that shouldn't be added
//          logger.warn("attempt to add old member: {} as surprise member to {}",
//              member, latestView);
//          try {
//            requestMemberRemoval(member,
//                "this member is no longer in the view but is initiating connections");
//          } catch (MembershipClosedException | MemberDisconnectedException e) {
//            // okay to ignore
//          }
//        }).start();
//        addShunnedMember(member);
//        return false;
//      }

      // Adding the member to this set ensures we won't remove it if a new
      // view comes in and it is still not visible.
      surpriseMembers.put(member, Long.valueOf(System.currentTimeMillis()));

      if (shutdownInProgress()) {
        // Force disconnect, esp. the TCPConduit
        String msg =
            "This distributed system is shutting down.";

        destroyMember(member, msg);
        return true; // allow during shutdown
      }

      if (isShunned(member)) {
        warn = true;
        surpriseMembers.remove(member);
      } else {

        // Ensure that the member is accounted for in the view
        // Conjure up a new view including the new member. This is necessary
        // because we are about to tell the listener about a new member, so
        // the listener should rightfully expect that the member is in our
        // membership view.

        // However, we put the new member at the end of the list. This
        // should ensure it is not chosen as an elder.
        // This will get corrected when the member finally shows up in the
        // view.
        MembershipView<ID> newMembers = new MembershipView<>(latestView, latestView.getViewId());
        newMembers.add(member);
        newMembers.makeUnmodifiable();
        latestView = newMembers;
      }
    } finally {
      latestViewWriteLock.unlock();
    }
    if (warn) { // fix for bug #41538 - deadlock while alerting
      logger.warn("Membership: Ignoring surprise connect from shunned member <{}>",
          member);
    } else {
      membershipListener.newMemberConnected(member);
    }
    return !warn;
  }

  @Override
  public void startupMessageFailed(MemberIdentifier mbr, String failureMessage) {

  }

  @Override
  public boolean testMulticast() {
    return false;
  }

  @Override
  public boolean isSurpriseMember(MemberIdentifier m) {
    return false;
  }

  @Override
  public QuorumChecker getQuorumChecker() {
    return null;
  }

  @Override
  public ID getCoordinator() {
    latestViewReadLock.lock();
    try {
      return latestView == null ? null : latestView.getCoordinator();
    } finally {
      latestViewReadLock.unlock();
    }
  }

  @Override
  public Set getMembersNotShuttingDown() {
    latestViewReadLock.lock();
    try {
      return latestView.getMembers().stream().filter(id -> !shutdownMembers.containsKey(id))
          .collect(
              Collectors.toSet());
    } finally {
      latestViewReadLock.unlock();
    }
  }

  @Override
  public void processMessage(Message msg) throws MemberShunnedException {
    handleOrDeferMessage(msg);
  }

  @Override
  public void checkCancelled() throws MembershipClosedException {
    if (cancelCriterion.isCancelInProgress()) {
      throw new MembershipClosedException("Distributed System is shutting down",
          cancelCriterion.generateCancelledException(shutdownCause));
    }
  }

  @Override
  public void waitIfPlayingDead() {
    if (playingDead) { // wellness test hook
      while (playingDead && !shutdownInProgress) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  @Override
  public boolean isJoining() {
    return isJoining;
  }

  @Override
  public ID[] getAllMembers(ID[] arrayType) {
    latestViewReadLock.lock();
    try {
      List<ID> keySet = latestView.getMembers();
      return keySet.toArray(arrayType);
    } finally {
      latestViewReadLock.unlock();
    }
  }

  @Override
  public boolean hasMember(ID member) {
    latestViewReadLock.lock();
    try {
      return latestView.contains(member);
    } finally {
      latestViewReadLock.unlock();
    }
  }

  @Override
  public void start() throws MemberStartupException {
    isJoining = true;
    try {
      join();
      hasJoined = true;
      lifecycleListener.joinCompleted(localAddress);
    } catch (IOException | InterruptedException e) {
      throw new MemberStartupException("problem starting up", e);
    } finally {
      isJoining = false;
    }
  }

  @Override
  public void setCloseInProgress() {
    isCloseInProgress = true;
  }

  @Override
  public <V> V doWithViewLocked(Supplier<V> function) {
    latestViewReadLock.lock();
    try {
      return function.get();
    } finally {
      latestViewReadLock.unlock();
    }
  }

  @Override
  public void waitForMessageState(MemberIdentifier member, Map state)
      throws InterruptedException, TimeoutException {
    // TODO not implemented
  }

  private boolean isShunnedOrNew(final ID m) {
    latestViewReadLock.lock();
    try {
      return shunnedMembers.containsKey(m) || isNew(m);
    } finally { // synchronized
      latestViewReadLock.unlock();
    }
  }

  // must be invoked under view read or write lock
  private boolean isNew(final ID m) {
    return !latestView.contains(m) && !surpriseMembers.containsKey(m);
  }


  @Override
  public Map<String, Long> getMessageState(MemberIdentifier member, boolean includeMulticast,
      Map result) {
    return null;
  }

  public void setListeners(MembershipListener<ID> membershipListener,
      MessageListener<ID> messageListener,
      LifecycleListener<ID> lifecycleListener) {
    this.membershipListener = membershipListener;
    this.messageListener = messageListener;
    this.lifecycleListener = lifecycleListener;
  }

  public void init() {

  }


  public void join()
      throws IOException, InterruptedException, MemberStartupException {
    // TODO: respect membershipConfig port settings
    // final int[] membershipPortRange = this.membershipConfig.getMembershipPortRange();
    // TODO use bind-address from config (membershipConfig.getBindAddress())

    // tell Rapid not to shut down the messenger if there's a problem joining
    System.setProperty("rapid.cluster.inhibitMessengerShutdown", "true");

    String hostname = membershipConfig.getBindAddress();
    if (hostname == null || hostname.trim().isEmpty()) {
      hostname = LocalHostUtil.getLocalHost().getCanonicalHostName();
    }
    final int port = startNettyAndGetPort(hostname);

    establishLocalAddress(hostname, port);
    logger.info("BRUCE: local address is {}", localAddress);

    // establish the serialized form of our identifier after the lifecycleListener has established
    // the directChannel port. Otherwise it's -1.
    Map<String, ByteString> metadata = new HashMap<>();
    metadata.put(GEODE_ID, objectToByteString(localAddress, serializer));

    listenAddress = HostAndPort.fromParts(hostname, port);

    boolean bypassDiscovery = Boolean.getBoolean(BYPASS_DISCOVERY_PROPERTY);

    // concurrent startup loop
    int tries = 0;
    long timeout = membershipConfig.getJoinTimeout();
    long giveupTime = System.currentTimeMillis() + timeout;

    final int triesBeforeVoting = 10;
    HostAndPort seedAddress = null;
    do {
      initializeClusterBuilder(metadata);
      if (bypassDiscovery) {
        logger.info("bypassDiscovery is set - starting new cluster");
        cluster = clusterBuilder.start();
        break;
      }
      try {
        logger.info("BRUCE: finding seed address from locator.  This is attempt #" + (tries + 1));
        seedAddress = findSeedAddressFromLocators();
        logger.info("BRUCE: seed address is " + seedAddress);
        if (!seedAddress.equals(listenAddress)) {
          logger.info("BRUCE: joining existing cluster");
          cluster = clusterBuilder.join(seedAddress);
          break;
        }
      } catch (MemberStartupException e) {
        if (membershipLocator == null) {
          messenger.shutdown();
          throw e;
        }
        tries++;
      } catch (Cluster.JoinException e) {
        tries++;
      }
      if (cluster == null) {
        if (tries >= triesBeforeVoting) {
          List listOfRegistrants = new ArrayList(registrants.size());
          listOfRegistrants.addAll(registrants);
          Collections.sort(listOfRegistrants);
          ID bestChoice = (ID) listOfRegistrants.get(0);
          seedAddress =
              HostAndPort.fromParts(bestChoice.getHostName(), bestChoice.getMembershipPort());
          logger.info("BRUCE: lead registrant address is " + seedAddress);
          try {
            if (bestChoice.equals(localAddress)) {
              logger.info("BRUCE: I am the lead registrant - starting new cluster");
              cluster = clusterBuilder.start();
            } else {
              logger.info("BRUCE: joining with lead registrant");
              cluster = clusterBuilder.join(seedAddress);
            }
          } catch (Cluster.JoinException e) {
            messenger.shutdown();
            throw new MemberStartupException("Unable to join the cluster", e);
          }
        } else {
          try {
            Thread.sleep(JOIN_RETRY_SLEEP);
          } catch (InterruptedException e) {
            messenger.shutdown();
            throw new MemberStartupException("join attempt was interrupted");
          }
        }
      }
    } while (cluster == null && System.currentTimeMillis() < giveupTime);

    if (cluster == null) {
      throw new MemberStartupException("unable to join the cluster in the allotted amount of time");
    }

    // at this point we have joined but may not (yet) have a membership view

    // todo need wait/notify or a Promise/Future of some kind here
    logger.info("BRUCE: waiting for initial membership view");
    while (latestView == null && giveupTime < System.currentTimeMillis()) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        disconnect(true);
        throw new MemberStartupException("interrupted waiting for the cluster membership view");
      }
    }
    if (latestView == null) {
      disconnect(true);
      throw new MemberStartupException("timed out waiting for the cluster membership view");
    }
    if (locator != null) {
      locator.setMembership(this);
    }
    logger.info("BRUCE: finished joining the cluster");
  }

  protected void initializeClusterBuilder(Map<String, ByteString> metadata) {
    clusterBuilder = new Cluster.Builder(listenAddress)
        .setMessagingClientAndServer(messenger, messenger)
        .setMetadata(metadata);
    clusterBuilder.addSubscription(com.vrg.rapid.ClusterEvents.VIEW_CHANGE_PROPOSAL,
        this::onViewChangeProposal);
    clusterBuilder.addSubscription(com.vrg.rapid.ClusterEvents.VIEW_CHANGE,
        this::onViewChange);
    clusterBuilder.addSubscription(com.vrg.rapid.ClusterEvents.KICKED,
        this::onKicked);
  }

  protected void establishLocalAddress(String hostname, int port) throws UnknownHostException {
    final boolean isLocator = (membershipLocator != null);
    GMSMemberData gmsMember = new GMSMemberData(LocalHostUtil.getLocalHost(),
        hostname, port,
        OSProcess.getId(), (byte) membershipConfig.getVmKind(),
        -1 /* directport */, 0 /* viewID */, membershipConfig.getName(),
        GMSUtil.parseGroups(membershipConfig.getRoles(), membershipConfig.getGroups()),
        membershipConfig.getDurableClientId(),
        membershipConfig.getDurableClientTimeout(),
        membershipConfig.isNetworkPartitionDetectionEnabled(), isLocator,
        KnownVersion.getCurrentVersion().ordinal(),
        System.currentTimeMillis(), System.nanoTime(),
        (byte) (membershipConfig.getMemberWeight() & 0xff), false, null);

    localAddress = memberFactory.create(gmsMember);
    lifecycleListener.start(localAddress);
  }

  protected int startNettyAndGetPort(String hostname) {
    final Endpoint endpoint = Endpoint.newBuilder()
        .setHostname(ByteString.copyFromUtf8(hostname))
        .setPort(0).build();
    messenger = new NettyClientServer(endpoint);
    messenger.start();
    return messenger.getBoundPort();
  }

  /**
   * When there is (probably) going to be a view change Rapid will invoke this callback.
   * Being concensus based, Rapid will not cast a new view if the number of nodes drops to
   * one, so we take this opportunity to detect that situation and cast a local view change.
   */
  private void onViewChangeProposal(long viewID, List<NodeStatusChange> nodeStatusChanges) {
    logger.info("BRUCE: " + localAddress + " Rapid.onViewChangeProposal: " + nodeStatusChanges);
    try {
      // if we're still joining we can punt to onViewChange to process the changes
      if (latestView == null) {
        return;
      }
      // if a new node is joining we can wait until onViewChange to process the changes
      boolean addingNewNode = nodeStatusChanges.stream()
          .anyMatch(nodeStatusChange -> nodeStatusChange.getStatus() == EdgeStatus.UP);
      if (addingNewNode) {
        return;
      }
      checkBecomeSoleMember(viewID, nodeStatusChanges);
    } catch (RuntimeException | Error t) {
      t.printStackTrace();
      throw t;
    }
  }

  private void checkBecomeSoleMember(long viewID, List<NodeStatusChange> nodeStatusChanges) {
    // here we try to detect whether the cluster has devolved to a single member (this node)
    MembershipView<ID> newView = createGeodeView(latestView, viewID, nodeStatusChanges);
    if (newView.size() == 1 && newView.contains(localAddress)) {
      logger.info("BRUCE: becoming sole member of the cluster", new Exception("stack trace"));
      handleOrDeferViewEvent(newView);
      cluster.shutdownForRestart();
      Map<String, ByteString> metadata = new HashMap<>();
      metadata.put(GEODE_ID, objectToByteString(localAddress, serializer));
      clusterBuilder.setMetadata(metadata);
      try {
        cluster = clusterBuilder.restart();
      } catch (IOException e) {
        setShutdown();
        final Exception shutdownCause =
            new MemberDisconnectedException("unable to restart cluster: " + e);
        logger.fatal(
            String.format("Membership service failure: %s", shutdownCause.getMessage()),
            shutdownCause);
        try {
          membershipListener.saveConfig();
        } finally {
          new LoggingThread("DisconnectThread", false, () -> {
            lifecycleListener.forcedDisconnect();
            uncleanShutdown(shutdownCause.getMessage(), shutdownCause);
          }).start();
        }
      }
    }
  }

  private synchronized void onViewChange(Long viewID, List<NodeStatusChange> nodeStatusChanges) {
    logger.info("BRUCE: " + localAddress + " Rapid.onViewChange: " + nodeStatusChanges);
    MembershipView<ID> currentView = latestView;
    if (firstView && !isConnected()) {
      firstView = false;
      latestView = createGeodeView(currentView, viewID, nodeStatusChanges);
      logger.info("BRUCE: created initial view {}", latestView);
      logger.debug("Membership: initial view is {}", latestView);
    } else {
      handleOrDeferViewEvent(createGeodeView(currentView, viewID, nodeStatusChanges));
    }
  }

  private MembershipView<ID> createGeodeView(MembershipView<ID> baseView, long viewID,
      List<NodeStatusChange> nodeStatusChanges) {
    MembershipView<ID> viewChanges =
        createGeodeView(this.localAddress, viewID, nodeStatusChanges);
    MembershipView<ID> newView = new MembershipView(viewChanges.getCreator(),
        (int) (viewID & 0x7FFFFFFF), baseView.getMembers());
    newView.removeAll(viewChanges.getCrashedMembers());
    newView.removeAll(viewChanges.getShutdownMembers());
    for (ID member : viewChanges.getMembers()) {
      if (!newView.contains(member)) {
        // todo here we could inform the View that this is a new member for its getNewMembers method
        logger.info("BRUCE: adding new member {}", member);
        newView.add(member);
      }
    }
    newView.makeUnmodifiable();
    logger.info("BRUCE: created new view from nodeStatusChanges: {}", newView);
    return newView;
  }

  private MembershipView<ID> createGeodeView(ID gmsCreator, long viewId,
      List<NodeStatusChange> nodeStatusChanges) {
    ID geodeCreator = gmsCreator;
    List<ID> upnodes = new ArrayList<>(nodeStatusChanges.size());
    Set<ID> downnodes = new HashSet<>(nodeStatusChanges.size());
    for (NodeStatusChange nodeStatusChange : nodeStatusChanges) {
      Metadata metadata = nodeStatusChange.getMetadata();
      ID geodeMember = (ID)byteStringToObject(metadata.getMetadataMap().get(GEODE_ID), serializer);
      if (nodeStatusChange.getStatus() == EdgeStatus.UP) {
        upnodes.add(geodeMember);
      } else {
        downnodes.add(geodeMember);
      }
      // TODO: since Rapid has no stable ordering in its views we attempt to impose one here based
      // on clock readings. This is not a long-term solution since clocks may not be sufficiently
      // synchronized
      Collections.sort(upnodes, (a, b) -> {
        long aMillis = a.getUuidMostSignificantBits();
        long bMillis = b.getUuidMostSignificantBits();
        if (aMillis < bMillis) {
          return -1;
        }
        if (bMillis > aMillis) {
          return 1;
        }
        long aNanos = a.getUuidLeastSignificantBits();
        long bNanos = b.getUuidLeastSignificantBits();
        if (aNanos < bNanos) {
          return -1;
        }
        if (bNanos > aNanos) {
          return 1;
        }
        return 0;
      });
    }
    // treat all missing members as having crashed. DistributionImpl will know if a
    // shutdown message has been received and can invoke the proper listener callback based
    // on that knowledge
    return new MembershipView<ID>(geodeCreator, (int) (viewId & 0x7FFFFFFF), upnodes,
        Collections.emptySet(), downnodes);
  }


  /**
   * Notification from Rapid that this node has been kicked out of the cluster
   */
  private void onKicked(Long viewID, List<NodeStatusChange> nodeStatusChanges) {
    {
      logger.info("BRUCE: Rapid.onKicked: " + nodeStatusChanges);
      if (shutdownInProgress || isJoining()) {
        return;
      }

      setShutdown();

      final Exception shutdownCause = new MemberDisconnectedException(nodeStatusChanges.toString());

      // cache the exception so it can be appended to ShutdownExceptions
      // services.setShutdownCause(shutdownCause);
      // services.getCancelCriterion().cancel(reason);

      // if (!inhibitForceDisconnectLogging) {
      logger.fatal(
          String.format("Membership service failure: %s", shutdownCause.getMessage()),
          shutdownCause);
      // }

      // todo will we continue to support auto-reconnect? It's useful for handling kicked-out
      // todo members due to GC pauses
      // if (this.isReconnectingDS()) {
      // logger.info("Reconnecting system failed to connect");
      // uncleanShutdown(reason,
      // new MemberDisconnectedException("reconnecting system failed to connect"));
      // return;
      // }

      try {
        membershipListener.saveConfig();
      } finally {
        new LoggingThread("DisconnectThread", false, () -> {
          lifecycleListener.forcedDisconnect();
          uncleanShutdown(shutdownCause.getMessage(), shutdownCause);
        }).start();
      }
    }
  }

  protected void handleOrDeferViewEvent(MembershipView<ID> viewArg) {
    if (locator != null) {
      locator.installView(viewArg);
    }
    if (this.isJoining) {
      // bug #44373 - queue all view messages while joining.
      // This is done under the latestViewLock, but we can't block here because
      // we're sitting in the UDP reader thread.
      synchronized (startupLock) {
        startupMessages.add(new StartupEvent<>(viewArg));
        return;
      }
    }
    latestViewWriteLock.lock();
    try {
      if (!processingEvents) {
        synchronized (startupLock) {
          if (!startupMessagesDrained) {
            startupMessages.add(new StartupEvent<>(viewArg));
            return;
          }
        }
      }
      viewExecutor.submit(() -> processView(viewArg));
    } finally {
      latestViewWriteLock.unlock();
    }

  }

  /**
   * Dispatch routine for processing a single startup event
   *
   * @param o the startup event to handle
   */
  private void processStartupEvent(StartupEvent<ID> o) {
    // Most common events first

    if (o.isDistributionMessage()) { // normal message
      try {
        o.dmsg.setSender(
            latestView.getCanonicalID(o.dmsg.getSender()));
        dispatchMessage(o.dmsg);
      } catch (MemberShunnedException e) {
        // message from non-member - ignore
      }
    } else if (o.isGmsView()) { // view event
      processView(o.gmsView);
    } else if (o.isSurpriseConnect()) { // connect
      addSurpriseMember(o.member);
    } else {
      throw new IllegalArgumentException("unknown startup event: " + o);
    }
  }

  public void processView(MembershipView<ID> newView) {
    // Sanity check...
    if (logger.isDebugEnabled()) {
      StringBuilder msg = new StringBuilder(200);
      msg.append("Membership: Processing view ");
      msg.append(newView);
      msg.append("} on ").append(localAddress.toString());
      logger.debug(msg);
      if (!newView.contains(localAddress)) {
        logger.info("The Member with id {}, is no longer in my own view, {}",
            localAddress, newView);
      }
    }

    // We perform the update under a global lock so that other
    // incoming events will not be lost in terms of our global view.
    latestViewWriteLock.lock();
    try {
      // Save previous view, for delta analysis
      MembershipView<ID> priorView = latestView;

      // TODO: Rapid configuration numbers are being used as view IDs. These numbers don't
      // get incremented sequentially like JGroups view IDs, so we can't compare them.
      // if (newView.getViewId() < priorView.getViewId()) {
      // // ignore this view since it is old news
      // return;
      // }

      // update the view to reflect our changes, so that
      // callbacks will see the new (updated) view.
      MembershipView<ID> newlatestView = new MembershipView<>(newView, newView.getViewId());

      // look for additions
      for (int i = 0; i < newView.getMembers().size(); i++) { // additions
        ID m = newView.getMembers().get(i);

        // Once a member has been seen via a view, remove them from the
        // newborn set. Replace the member data of the surpriseMember ID
        // in case it was a partial ID and is being retained by DistributionManager
        // or some other object
        boolean wasSurprise = surpriseMembers.containsKey(m);
        if (wasSurprise) {
          for (Iterator<Map.Entry<ID, Long>> iterator =
              surpriseMembers.entrySet().iterator(); iterator.hasNext();) {
            Map.Entry<ID, Long> entry = iterator.next();
            if (entry.getKey().equals(m)) {
              entry.getKey().setMemberData(m.getMemberData());
              iterator.remove();
              break;
            }
          }
        }

        // if it's in a view, it's no longer suspect
        suspectedMembers.remove(m);

        if (priorView.contains(m) || wasSurprise) {
          continue; // already seen
        }

        // unblock any waiters for this particular member.
        // i.e. signal any waiting threads in tcpconduit.
        String authInit =
            membershipConfig.getSecurityPeerAuthInit();
        boolean isSecure = authInit != null && authInit.length() != 0;

        if (shutdownInProgress()) {
          addShunnedMember(m);
          continue; // no additions processed after shutdown begins
        } else {
          boolean wasShunned = endShun(m); // bug #45158 - no longer shun a process that is now in
          // view
          if (wasShunned && logger.isDebugEnabled()) {
            logger.debug("No longer shunning {} as it is in the current membership view", m);
          }
        }

        logger.info("Membership: Processing addition <{}>", m);

        membershipListener.newMemberConnected(m);
      } // additions

      // look for departures
      for (int i = 0; i < priorView.getMembers().size(); i++) { // departures
        ID m = priorView.getMembers().get(i);
        if (newView.contains(m)) {
          continue; // still alive
        }

        if (surpriseMembers.containsKey(m)) {
          continue; // member has not yet appeared in a view
        }

        try {
          removeWithViewLock(m,
              newView.getCrashedMembers().contains(m) || suspectedMembers.containsKey(m),
              "departed membership view");
        } catch (VirtualMachineError err) {
          // If this ever returns, rethrow the error. We're poisoned
          // now, so don't let this thread continue.
          throw err;
        } catch (Throwable t) {
          logger.info(String.format("Membership: Fault while processing view removal of %s",
              m),
              t);
        }
      } // departures

      // expire surprise members, add others to view
      long oldestAllowed = System.currentTimeMillis() - this.surpriseMemberTimeout;
      for (Iterator<Map.Entry<ID, Long>> it =
          surpriseMembers.entrySet().iterator(); it.hasNext();) {
        Map.Entry<ID, Long> entry = it.next();
        Long birthtime = entry.getValue();
        if (birthtime.longValue() < oldestAllowed) {
          it.remove();
          ID m = entry.getKey();
          logger.info("Membership: expiring membership of surprise member <{}>",
              m);
          removeWithViewLock(m, true,
              "not seen in membership view in " + this.surpriseMemberTimeout + "ms");
        } else {
          if (!newlatestView.contains(entry.getKey())) {
            newlatestView.add(entry.getKey());
          }
        }
      }
      // expire suspected members
      final long suspectMemberTimeout = 180000;
      oldestAllowed = System.currentTimeMillis() - suspectMemberTimeout;
      for (Iterator<Map.Entry<ID, Long>> it = suspectedMembers.entrySet().iterator(); it
          .hasNext();) {
        Map.Entry<ID, Long> entry = it.next();
        Long birthtime = entry.getValue();
        if (birthtime.longValue() < oldestAllowed) {
          it.remove();
        }
      }

      // the view is complete - let's install it
      newlatestView.makeUnmodifiable();
      latestView = newlatestView;
      membershipListener.viewInstalled(latestView);
    } finally {
      latestViewWriteLock.unlock();
    }
  }

  /**
   * Remove a member. {@link #latestViewLock} must be held before this method is called. If member
   * is not already shunned, the uplevel event handler is invoked.
   */
  private void removeWithViewLock(ID dm, boolean crashed, String reason) {
    boolean wasShunned = isShunned(dm);

    // Delete resources
    destroyMember(dm, reason);

    if (wasShunned) {
      return; // Explicit deletion, no upcall.
    }

    if (!shutdownMembers.containsKey(dm)) {
      // if we've received a shutdown message then DistributionManager will already have
      // notified listeners
      membershipListener.memberDeparted(dm, crashed, reason);
    }
  }

  /**
   * Add the given member to the shunned list. Also, purge any shunned members that are really
   * really old.
   * <p>
   * Must be called with {@link #latestViewLock} held and the view stable.
   *
   * @param m the member to add
   */
  private void addShunnedMember(ID m) {
    long deathTime = System.currentTimeMillis() - SHUNNED_SUNSET * 1000L;

    surpriseMembers.remove(m); // for safety

    // Update the shunned set.
    if (!isShunned(m)) {
      shunnedMembers.put(m, Long.valueOf(System.currentTimeMillis()));
    }

    // Remove really really old shunned members.
    // First, make a copy of the old set. New arrivals _a priori_ don't matter,
    // and we're going to be updating the list so we don't want to disturb
    // the iterator.
    Set<Map.Entry<ID, Long>> oldMembers = new HashSet<>(shunnedMembers.entrySet());

    Set<ID> removedMembers = new HashSet<>();

    for (final Map.Entry<ID, Long> oldMember : oldMembers) {
      Map.Entry<ID, Long> e = oldMember;

      // Key is the member. Value is the time to remove it.
      long ll = e.getValue().longValue();
      if (ll >= deathTime) {
        continue; // too new.
      }

      ID mm = e.getKey();

      if (latestView.contains(mm)) {
        // Fault tolerance: a shunned member can conceivably linger but never
        // disconnect.
        //
        // We may not delete it at the time that we shun it because the view
        // isn't necessarily stable. (Note that a well-behaved cache member
        // will depart on its own accord, but we force the issue here.)
        destroyMember(mm, "shunned but never disconnected");
      }
      if (logger.isDebugEnabled()) {
        logger.debug("Membership: finally removed shunned member entry <{}>", mm);
      }

      removedMembers.add(mm);
    }

    // removed timed-out entries from the shunned-members collections
    if (removedMembers.size() > 0) {
      for (final ID removedMember : removedMembers) {
        ID idm = removedMember;
        endShun(idm);
      }
    }
  }

  private boolean endShun(ID m) {
    boolean wasShunned = (shunnedMembers.remove(m) != null);
    shunnedAndWarnedMembers.remove(m);
    return wasShunned;
  }



  // TODO: implement locator-wait-time. If locators can't be contacted don't create a new
  // cluster unless blah blah blah circumstances exist (bypass discovery system property used
  // in GMSJoinLeave is set to true, for instance)
  private HostAndPort findSeedAddressFromLocators() throws MemberStartupException {
    assert this.localAddress != null;

    FindCoordinatorRequest<ID> request = new FindCoordinatorRequest<>(this.localAddress,
        new HashSet<>(), -1, null /* DH public key */,
        -1, null);


    int connectTimeout = (int) membershipConfig.getMemberTimeout() * 2;

    logger.debug("sending {} to {}", request, membershipConfig.getLocators());

    final List<org.apache.geode.distributed.internal.tcpserver.HostAndPort> locators =
        GMSUtil.parseLocators(membershipConfig.getLocators(), membershipConfig.getBindAddress());
    addLocalLocatorIfMissing(locators);
    registrants = new HashSet<>();
    for (org.apache.geode.distributed.internal.tcpserver.HostAndPort locator : locators) {
      try {
        Object o = locatorClient.requestToServer(locator, request, connectTimeout, true);
        FindCoordinatorResponse<ID> response =
            (o instanceof FindCoordinatorResponse) ? (FindCoordinatorResponse<ID>) o : null;
        if (response != null) {
          logger.info("received {} from locator {}", response, locator);
          ID responseCoordinator = response.getCoordinator();
          if (responseCoordinator != null) {
            return HostAndPort.fromParts(responseCoordinator.getHostName(),
                responseCoordinator.getMembershipPort());
          }
          registrants.addAll(response.getRegistrants());
        }
      } catch (ConnectException e) {
        // unable to contact this locator
        logger.info("Locator {} appears to be offline", locator);
      } catch (Exception e) {
        logger.info("Exception trying to contact locator " + locator, e);
      }
    }
    throw new MemberStartupException("unable to contact any locators");
  }

  // todo this needs work - it's not detecting that the fqdn:port of the local locator is
  // todo already in the list
  private void addLocalLocatorIfMissing(
      List<org.apache.geode.distributed.internal.tcpserver.HostAndPort> locators)
      throws MembershipConfigurationException {
    if (membershipLocator != null) {
      org.apache.geode.distributed.internal.tcpserver.HostAndPort localLocator =
          new org.apache.geode.distributed.internal.tcpserver.HostAndPort(null,
              membershipLocator.getPort());
      if (locators.contains(localLocator)) {
        return;
      }
      String hostName = membershipConfig.getBindAddress();
      if (hostName == null) {
        return;
      }
      localLocator =
          new org.apache.geode.distributed.internal.tcpserver.HostAndPort(hostName,
              membershipLocator.getPort());
      if (!locators.contains(localLocator)) {
        locators.add(localLocator);
      }
    }
  }

  /**
   * Dispatch the distribution message, or place it on the startup queue.
   *
   * @param msg the message to process
   */
  protected void handleOrDeferMessage(Message<ID> msg) throws MemberShunnedException {
    if (msg.dropMessageWhenMembershipIsPlayingDead() && (beingSick || playingDead)) {
      return;
    }
    if (!processingEvents) {
      synchronized (startupLock) {
        if (!startupMessagesDrained) {
          startupMessages.add(new StartupEvent<>(msg));
          return;
        }
      }
    }
    dispatchMessage(msg);
  }

  /**
   * Logic for processing a distribution message.
   * <p>
   * It is possible to receive messages not consistent with our view. We handle this here, and
   * generate an uplevel event if necessary
   *
   * @param msg the message
   */
  protected void dispatchMessage(Message<ID> msg) throws MemberShunnedException {
    ID m = msg.getSender();
    boolean shunned = false;

    // If this member is shunned or new, grab the latestViewWriteLock: update the appropriate data
    // structure.
    if (isShunnedOrNew(m)) {
      latestViewWriteLock.lock();
      try {
        if (isShunned(m)) {
          if (msg instanceof StopShunningMarker) {
            endShun(m);
          } else {
            // fix for bug 41538 - sick alert listener causes deadlock
            // due to view latestViewReadWriteLock being held during messaging
            shunned = true;
          }
        }

        if (!shunned) {
          // If it's a new sender, wait our turn, generate the event
          if (isNew(m)) {
            shunned = !addSurpriseMember(m);
          }
        }
      } finally {
        latestViewWriteLock.unlock();
      }
    }

    if (shunned) { // bug #41538 - shun notification must be outside synchronization to avoid
      // hanging
      warnShun(m);
      if (logger.isTraceEnabled()) {
        logger.trace("Membership: Ignoring message from shunned member <{}>:{}", m, msg);
      }
      throw new MemberShunnedException();
    }

    messageListener.messageReceived(msg);
  }

  static class StartupEvent<ID extends MemberIdentifier> {
    static final int SURPRISE_CONNECT = 1;
    static final int VIEW = 2;
    static final int MESSAGE = 3;

    /**
     * indicates whether the event is a departure, a surprise connect (i.e., before the view message
     * arrived), a view, or a regular message
     *
     * @see #SURPRISE_CONNECT
     * @see #VIEW
     * @see #MESSAGE
     */
    private final int kind;

    // Miscellaneous state depending on the kind of event
    ID member;
    Message<ID> dmsg;
    MembershipView<ID> gmsView;

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("kind=");
      switch (kind) {
        case SURPRISE_CONNECT:
          sb.append("connect; member = <").append(member).append(">");
          break;
        case VIEW:
          String text = gmsView.toString();
          sb.append("view <").append(text).append(">");
          break;
        case MESSAGE:
          sb.append("message <").append(dmsg).append(">");
          break;
        default:
          sb.append("unknown=<").append(kind).append(">");
          break;
      }
      return sb.toString();
    }

    /**
     * Create a surprise connect event
     *
     * @param member the member connecting
     */
    StartupEvent(final ID member) {
      this.kind = SURPRISE_CONNECT;
      this.member = member;
    }

    /**
     * Indicate if this is a surprise connect event
     *
     * @return true if this is a connect event
     */
    boolean isSurpriseConnect() {
      return this.kind == SURPRISE_CONNECT;
    }

    /**
     * Create a view event
     *
     * @param v the new view
     */
    StartupEvent(MembershipView<ID> v) {
      this.kind = VIEW;
      this.gmsView = v;
    }

    /**
     * Indicate if this is a view event
     *
     * @return true if this is a view event
     */
    boolean isGmsView() {
      return this.kind == VIEW;
    }

    /**
     * Create a message event
     *
     * @param d the message
     */
    StartupEvent(Message<ID> d) {
      this.kind = MESSAGE;
      this.dmsg = d;
    }

    /**
     * Indicate if this is a message event
     *
     * @return true if this is a message event
     */
    boolean isDistributionMessage() {
      return this.kind == MESSAGE;
    }
  }

  public class Stopper {
    volatile String reasonForStopping = null;

    public void cancel(String reason) {
      this.reasonForStopping = reason;
    }

    public String cancelInProgress() {
      if (shutdownCause != null)
        return shutdownCause.toString();
      return this.reasonForStopping;
    }

    public RuntimeException generateCancelledException(Throwable e) {
      if (shutdownCause instanceof MemberDisconnectedException) {
        MembershipClosedException newException =
            new MembershipClosedException("membership shutdown",
                e);
        throw newException;
      }
      String reason = cancelInProgress();
      if (reason == null) {
        return null;
      } else {
        if (e == null) {
          return new MembershipClosedException(reason);
        } else {
          return new MembershipClosedException(reason, e);
        }
      }
    }

    public boolean isCancelInProgress() {
      return cancelInProgress() != null;
    }

    public void checkCancelInProgress(Throwable e) {
      String reason = cancelInProgress();
      if (reason == null) {
        return;
      }
      throw generateCancelledException(e);
    }

  }

  public ByteString objectToByteString(DataSerializableFixedID object, DSFIDSerializer serializer) {
    BufferDataOutputStream out = new BufferDataOutputStream(KnownVersion.CURRENT);
    SerializationContext context = serializer.createSerializationContext(out);
    try {
      context.getSerializer().writeObject(object, out);
    } catch (IOException e) {
      throw new SerializationException("error serializing an identifier", e);
    }
    return ByteString.copyFrom(out.toByteBuffer());
  }

  /**
   * Protobuf deserialization
   */
  public DataSerializableFixedID byteStringToObject(ByteString byteString, DSFIDSerializer serializer) {
    ByteArrayDataInput input = new ByteArrayDataInput(byteString.toByteArray());
    DeserializationContext context = serializer.createDeserializationContext(input);
    try {
      return serializer.getObjectDeserializer().readObject(input);
    } catch (IOException | ClassNotFoundException e) {
      throw new SerializationException("error serializing an identifier", e);
    }
  }


}
