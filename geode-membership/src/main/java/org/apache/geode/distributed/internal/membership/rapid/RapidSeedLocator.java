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

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.Logger;

import org.apache.geode.distributed.internal.membership.api.MemberIdentifier;
import org.apache.geode.distributed.internal.membership.api.MembershipConfigurationException;
import org.apache.geode.distributed.internal.membership.api.MembershipLocatorStatistics;
import org.apache.geode.distributed.internal.membership.api.MembershipView;
import org.apache.geode.distributed.internal.membership.gms.GMSMembershipView;
import org.apache.geode.distributed.internal.membership.gms.GMSUtil;
import org.apache.geode.distributed.internal.membership.gms.locator.FindCoordinatorRequest;
import org.apache.geode.distributed.internal.membership.gms.locator.FindCoordinatorResponse;
import org.apache.geode.distributed.internal.membership.gms.locator.GetViewRequest;
import org.apache.geode.distributed.internal.membership.gms.locator.GetViewResponse;
import org.apache.geode.distributed.internal.membership.gms.messenger.GMSMemberWrapper;
import org.apache.geode.distributed.internal.tcpserver.HostAndPort;
import org.apache.geode.distributed.internal.tcpserver.TcpClient;
import org.apache.geode.distributed.internal.tcpserver.TcpHandler;
import org.apache.geode.distributed.internal.tcpserver.TcpServer;
import org.apache.geode.internal.serialization.ObjectDeserializer;
import org.apache.geode.internal.serialization.ObjectSerializer;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * GMSLocator responds to requests to find the membership coordinator in a cluster.
 * While a Service, it has a complicated life-cycle because it must be started before
 * Services have joined the cluster.
 *
 * @param <ID>
 */
public class RapidSeedLocator<ID extends MemberIdentifier> implements TcpHandler {

  static final int LOCATOR_FILE_STAMP = 0x7b8cf741;

  private static final Logger logger = LogService.getLogger();

  private final boolean usePreferredCoordinators;
  private final boolean networkPartitionDetectionEnabled;
  private final String securityUDPDHAlgo;
  private final String locatorString;
  private final List<HostAndPort> locators;
  private final MembershipLocatorStatistics locatorStats;
  private final Set<ID> registrants = new HashSet<>();
  private final Map<GMSMemberWrapper, byte[]> publicKeys =
      new ConcurrentHashMap<>();
  private final Path workingDirectory;
  private final ObjectSerializer objectSerializer;
  private final ObjectDeserializer objectDeserializer;

  private volatile boolean isCoordinator;

  private ID localAddress;

  /**
   * The current membership view, or one recovered from disk. This is a copy-on-write variable.
   */
  private GMSMembershipView<ID> view;

  private GMSMembershipView<ID> recoveredView;

  private File viewFile;
  private final TcpClient locatorClient;
  private MembershipImpl membership;

  /**
   * @param bindAddress network address that TcpServer will bind to
   * @param locatorString location of other locators (bootstrapping, failover)
   * @param usePreferredCoordinators true if the membership coordinator should be a Locator
   * @param networkPartitionDetectionEnabled true if network partition detection is enabled
   * @param locatorStats the locator statistics object
   * @param securityUDPDHAlgo DF algorithm
   * @param workingDirectory directory to use for view file (defaults to "user.dir")
   * @param objectSerializer a serializer used to persist the membership view
   * @param objectDeserializer a deserializer used to recover the membership view
   */
  public RapidSeedLocator(InetAddress bindAddress, String locatorString,
      boolean usePreferredCoordinators,
      boolean networkPartitionDetectionEnabled, MembershipLocatorStatistics locatorStats,
      String securityUDPDHAlgo, Path workingDirectory, final TcpClient locatorClient,
      ObjectSerializer objectSerializer,
      ObjectDeserializer objectDeserializer)
      throws MembershipConfigurationException {
    this.usePreferredCoordinators = usePreferredCoordinators;
    this.networkPartitionDetectionEnabled = networkPartitionDetectionEnabled;
    this.securityUDPDHAlgo = securityUDPDHAlgo;
    this.locatorString = locatorString;
    if (this.locatorString == null || this.locatorString.isEmpty()) {
      locators = new ArrayList<>(0);
    } else {
      locators = GMSUtil.parseLocators(locatorString, bindAddress);
    }
    this.locatorStats = locatorStats;
    this.workingDirectory = workingDirectory;
    this.locatorClient = locatorClient;
    this.objectSerializer = objectSerializer;
    this.objectDeserializer = objectDeserializer;
  }

  /**
   * Called initially and after each auto-reconnect. See restart handlers in InternalLocator
   * up in geode-core. Services must be started before this call.
   *
   */
  public synchronized boolean setMembership(
      final MembershipImpl<ID> membership) {
    if (this.membership == null || !this.membership.isConnected()) {
      this.membership = membership;
      localAddress = membership.getLocalMember();
      Objects.requireNonNull(localAddress, "member address should have been established");
      logger.info("Peer locator is connecting to local membership services with ID {}",
          localAddress);
      MembershipView<ID> newView = membership.getView();
      if (newView != null) {
        view = new GMSMembershipView<ID>(newView.getCreator(), newView.getViewId(),
            newView.getMembers());
        recoveredView = null;
      } else {
        // if we are auto-reconnecting we may already have a membership view
        // and should use it as a "recovered view" which only hints at who is
        // the current membership coordinator
        if (view != null) {
          view.setViewId(-100); // no longer a valid view
          recoveredView = view; // auto-reconnect will be based on the recovered view
          view = null;
        }
        if (localAddress != null) {
          if (recoveredView != null) {
            recoveredView.remove(localAddress);
          }
          synchronized (registrants) {
            registrants.add(localAddress);
          }
        }
      }
      notifyAll();
      return true;
    }
    return false;
  }

  public void init(TcpServer server) {
    String persistentFileIdentifier = "" + server.getPort();
    if (viewFile == null) {
      viewFile =
          workingDirectory.resolve("locator" + persistentFileIdentifier + "view.dat").toFile();
    }
    logger.info(
        "GemFire peer location service starting.  Other locators: {}  Locators preferred as coordinators: {}  Network partition detection enabled: {}  View persistence file: {}",
        locatorString, usePreferredCoordinators, networkPartitionDetectionEnabled, viewFile);

    recoverFromOtherLocators();
  }

  public void installView(MembershipView<ID> view) {
    logger.info("Peer locator received new membership view: {}", view);
    this.view = new GMSMembershipView<ID>(view.getCreator(), view.getViewId(), view.getMembers());
    synchronized (registrants) {
      registrants.clear();
    }
  }

  public Object processRequest(Object request) {
    if (logger.isDebugEnabled()) {
      logger.debug("Peer locator processing {}", request);
    }

    if (localAddress == null && membership != null) {
      localAddress = (ID) membership.getLocalMember();
    }

    Object response = null;
    if (request instanceof GetViewRequest) {
      logger.info("BRUCE: processing a GetViewRequest");
      if (view != null) {
        response = new GetViewResponse<>(new GMSMembershipView<ID>(view.getCreator(),
            view.getViewId(), view.getMembers()));
      }
      logger.info("BRUCE: returning view " + view);
    } else if (request instanceof FindCoordinatorRequest) {
      response = processFindCoordinatorRequest((FindCoordinatorRequest<ID>) request);
    }
    if (logger.isDebugEnabled()) {
      logger.debug("Peer locator returning {}", response);
    }
    return response;
  }

  private FindCoordinatorResponse<ID> processFindCoordinatorRequest(
      FindCoordinatorRequest<ID> findRequest) {
    if (findRequest.getMemberID() == null) {
      return null;
    }
    GMSMembershipView<ID> responseView = view;
    if (responseView == null) {
      responseView = recoveredView;
    }

    synchronized (registrants) {
      registrants.add(findRequest.getMemberID());
      if (recoveredView != null) {
        recoveredView.remove(findRequest.getMemberID());
      }
    }

    ID coordinator = null;
    boolean fromView = false;
    if (responseView != null) {
      // if the ID of the requester matches an entry in the membership view then remove
      // that entry - it's obviously an old member since the ID has been reused
      ID requestingMemberID = findRequest.getMemberID();
      for (ID id : responseView.getMembers()) {
        if (requestingMemberID.getMemberData().compareTo(id.getMemberData(), false) == 0) {
          GMSMembershipView<ID> newView =
              new GMSMembershipView<>(responseView, responseView.getViewId());
          newView.remove(id);
          responseView = newView;
          break;
        }
      }

      if (responseView.getViewId() > findRequest.getLastViewId()) {
        // ignore the requests rejectedCoordinators if the view has changed
        coordinator = responseView.getCoordinator(Collections.emptyList());
      } else {
        coordinator = responseView.getCoordinator(findRequest.getRejectedCoordinators());
      }
      logger.info("Peer locator: coordinator from view is {}", coordinator);
      fromView = true;
    }

    if (coordinator == null) {
      // find the "oldest" registrant
      Collection<ID> rejections = findRequest.getRejectedCoordinators();
      if (rejections == null) {
        rejections = Collections.emptyList();
      }

      if (membership != null) {
        synchronized (registrants) {
          coordinator = (ID) membership.getLocalMember().getMemberData();
          for (ID mbr : registrants) {
            if (mbr != coordinator && (coordinator == null || Objects.compare(mbr, coordinator,
                membership.getMemberFactory().getComparator()) < 0)) {
              if (!rejections.contains(mbr) && (mbr.preferredForCoordinator()
                  || !mbr.isNetworkPartitionDetectionEnabled())) {
                coordinator = mbr;
              }
            }
          }
          logger.info("Peer locator: coordinator from registrations is {}", coordinator);
        }
      }
    }

    synchronized (registrants) {
      if (isCoordinator) {
        coordinator = localAddress;
        if (responseView != null && localAddress != null
            && !localAddress.equals(responseView.getCoordinator())) {
          responseView = null;
          fromView = false;
        }
      }

      return new FindCoordinatorResponse<ID>(coordinator, localAddress, fromView, responseView,
          new HashSet<>(registrants), networkPartitionDetectionEnabled, usePreferredCoordinators,
          null);
    }
  }


  public void endRequest(Object request, long startTime) {
    locatorStats.endLocatorRequest(startTime);
  }

  public void endResponse(Object request, long startTime) {
    locatorStats.endLocatorResponse(startTime);
  }

  public void shutDown() {}

  private boolean recoverFromOtherLocators() {
    for (HostAndPort other : locators) {
      if (recover(other)) {
        logger.info("Peer locator recovered state from {}", other);
        return true;
      }
    }
    return false;
  }

  private boolean recover(HostAndPort other) {
    try {
      logger.info("Peer locator attempting to recover from {}", other);
      Object response = locatorClient.requestToServer(other,
          new GetViewRequest(), 20000, true);
      if (response instanceof GetViewResponse) {
        view = ((GetViewResponse<ID>) response).getView();
        logger.info("Peer locator recovered initial membership of {}", view);
        return true;
      }
    } catch (IOException | ClassNotFoundException e) {
      logger.debug("Peer locator could not recover membership view from {}: {}", other,
          e.getMessage());
    }
    logger.info("Peer locator was unable to recover state from this locator");
    return false;
  }



}
