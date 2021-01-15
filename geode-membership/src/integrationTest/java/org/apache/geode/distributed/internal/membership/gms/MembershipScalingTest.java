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
package org.apache.geode.distributed.internal.membership.gms;

import static org.apache.geode.distributed.internal.membership.api.MembershipConfig.DEFAULT_LOCATOR_WAIT_TIME;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.distributed.internal.membership.api.MemberIdentifier;
import org.apache.geode.distributed.internal.membership.api.MemberIdentifierFactoryImpl;
import org.apache.geode.distributed.internal.membership.api.MemberStartupException;
import org.apache.geode.distributed.internal.membership.api.Membership;
import org.apache.geode.distributed.internal.membership.api.MembershipBuilder;
import org.apache.geode.distributed.internal.membership.api.MembershipConfig;
import org.apache.geode.distributed.internal.membership.api.MembershipConfigurationException;
import org.apache.geode.distributed.internal.membership.api.MembershipLocator;
import org.apache.geode.distributed.internal.membership.api.MembershipLocatorBuilder;
import org.apache.geode.distributed.internal.tcpserver.TcpClient;
import org.apache.geode.distributed.internal.tcpserver.TcpSocketCreator;
import org.apache.geode.distributed.internal.tcpserver.TcpSocketCreatorImpl;
import org.apache.geode.distributed.internal.tcpserver.TcpSocketFactory;
import org.apache.geode.internal.inet.LocalHostUtil;
import org.apache.geode.internal.serialization.DSFIDSerializer;
import org.apache.geode.internal.serialization.internal.DSFIDSerializerImpl;
import org.apache.geode.logging.internal.executors.LoggingExecutors;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

/**
 * Tests of using the membership APIs to make multiple Membership systems that communicate
 * with each other and form a group
 */
public class MembershipScalingTest {
  private InetAddress localHost;
  private DSFIDSerializer dsfidSerializer;
  private TcpSocketCreator socketCreator;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public ExecutorServiceRule executorServiceRule = new ExecutorServiceRule();

  @Before
  public void before() throws IOException, MembershipConfigurationException {
    localHost = LocalHostUtil.getLocalHost();
    dsfidSerializer = new DSFIDSerializerImpl();
    socketCreator = new TcpSocketCreatorImpl();
  }

  @Test
  public void manyMembershipsCanStartWithOneLocator()
      throws IOException, MemberStartupException {
    final String memberPrefix = "member";
    final int numberOfMembers = 100;
    final Membership<MemberIdentifier>[] members = new Membership[numberOfMembers + 1];

    final MembershipLocator<MemberIdentifier> locator = createLocator(memberPrefix + 1, 0);
    locator.start();

    final int locatorPort = locator.getPort();
    final String threadName = Thread.currentThread().getName();

    final long[] startupTimes = new long[numberOfMembers + 1];
    long shutdownMillis = 0;
    int numberOfNodesStarted = 0;
    try {
      try {
        // set a new thread name to aid in debugging failures
        Thread.currentThread().setName("starting membership 1");
        members[0] = createMembership(memberPrefix + "1", locator, locatorPort);
        start(members[0]);
      } finally {
        Thread.currentThread().setName(threadName);
      }

      long startupStartTime = 0;
      for (int memberNumber = 1; memberNumber <= numberOfMembers; memberNumber++) {
        try {
          Thread.currentThread().setName("starting membership " + (memberNumber + 1));
          members[memberNumber] =
              createMembership(memberPrefix + (memberNumber + 1), null, locatorPort);
          startupStartTime = System.currentTimeMillis();
          start(members[memberNumber]);
          startupTimes[memberNumber] = System.currentTimeMillis() - startupStartTime;
          System.out.println("BRUCE: startup of member " + (memberNumber + 1) + " took "
              + startupTimes[memberNumber]);
          numberOfNodesStarted++;
        } finally {
          Thread.currentThread().setName(threadName);
        }
      }
      await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
        for (int memberNumber = 0; memberNumber <= numberOfMembers; memberNumber++) {
          // System.out.println(
          // "waiting for membership " + (memberNumber + 1) + " to have " + (numberOfMembers+1) + "
          // nodes");
          assertThat(members[memberNumber].getView().getMembers())
              .hasSize(numberOfMembers + 1);
        }
      });

    } finally {
      try {
        long shutdownStartTime = System.currentTimeMillis();
        stop(members);
        shutdownMillis = System.currentTimeMillis() - shutdownStartTime;
        stop(locator);
        System.out.printf("Shutdown took %s\n", shutdownMillis);
      } finally {
        long startupMillis = Arrays.stream(startupTimes).filter(each -> each != 0).sum();
        long averageStartup = startupMillis / numberOfNodesStarted;
        double squareSums =
            Arrays.stream(startupTimes).filter(each -> each != 0).asDoubleStream()
                .map(each -> Math.pow(each - averageStartup, 2.0)).sum();
        double stddev = Math.sqrt(squareSums / (numberOfNodesStarted - 1.0));
        long maxStartup = Arrays.stream(startupTimes).max().getAsLong();
        System.out.printf("Startup of %s took %s with an average of %s, a max of %s ",
            numberOfNodesStarted, startupMillis, averageStartup, maxStartup);
        System.out.printf("and a standard deviation of %g.\n", stddev);
        assertThat(Arrays.stream(startupTimes).filter(each -> each != 0).count())
            .isEqualTo(numberOfNodesStarted);
      }
    }
  }

  private CompletableFuture<Membership<MemberIdentifier>> launchLocator(
      Supplier<ExecutorService> executorServiceSupplier, int locatorPort, MembershipConfig config) {
    return executorServiceRule.supplyAsync(() -> {
      try {
        Path locatorDirectory0 = temporaryFolder.newFolder().toPath();
        MembershipLocator<MemberIdentifier> locator0 =
            MembershipLocatorBuilder.newLocatorBuilder(
                socketCreator,
                dsfidSerializer,
                locatorDirectory0,
                executorServiceSupplier)
                .setConfig(config)
                .setPort(locatorPort)
                .create();
        locator0.start();

        Membership<MemberIdentifier> membership = createMembership(config, locator0);
        membership.start();
        membership.startEventProcessing();
        return membership;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
  }

  private void start(final Membership<MemberIdentifier> membership)
      throws MemberStartupException {
    membership.start();
    membership.startEventProcessing();
  }

  private Membership<MemberIdentifier> createMembership(
      final String memberName,
      final MembershipLocator<MemberIdentifier> embeddedLocator,
      final int... locatorPorts)
      throws MembershipConfigurationException {
    final boolean isALocator = embeddedLocator != null;
    final MembershipConfig config =
        createMembershipConfig(isALocator, DEFAULT_LOCATOR_WAIT_TIME, memberName, locatorPorts);
    return createMembership(config, embeddedLocator);
  }

  private Membership<MemberIdentifier> createMembership(
      final MembershipConfig config,
      final MembershipLocator<MemberIdentifier> embeddedLocator)
      throws MembershipConfigurationException {
    final MemberIdentifierFactoryImpl memberIdFactory = new MemberIdentifierFactoryImpl();

    final TcpClient locatorClient =
        new TcpClient(socketCreator, dsfidSerializer.getObjectSerializer(),
            dsfidSerializer.getObjectDeserializer(), TcpSocketFactory.DEFAULT);

    return MembershipBuilder.<MemberIdentifier>newMembershipBuilder(
        socketCreator, locatorClient, dsfidSerializer, memberIdFactory)
        .setMembershipLocator(embeddedLocator)
        .setConfig(config)
        .create();
  }

  private MembershipConfig createMembershipConfig(
      final boolean isALocator,
      final int locatorWaitTime,
      final String memberName,
      final int... locatorPorts) {
    return new MembershipConfig() {
      public String getLocators() {
        return getLocatorString(locatorPorts);
      }

      // TODO - the Membership system starting in the locator *MUST* be told that is
      // is a locator through this flag. Ideally it should be able to infer this from
      // being associated with a locator
      @Override
      public int getVmKind() {
        return isALocator ? MemberIdentifier.LOCATOR_DM_TYPE : MemberIdentifier.NORMAL_DM_TYPE;
      }

      @Override
      public int getLocatorWaitTime() {
        return locatorWaitTime;
      }

      @Override
      public String getName() {
        return memberName;
      }
    };
  }

  private String getLocatorString(
      final int... locatorPorts) {
    final String hostName = localHost.getHostName();
    return Arrays.stream(locatorPorts)
        .mapToObj(port -> hostName + '[' + port + ']')
        .collect(Collectors.joining(","));
  }

  private MembershipLocator<MemberIdentifier> createLocator(
      String memberName,
      final int localPort,
      final int... locatorPorts)
      throws MembershipConfigurationException,
      IOException {
    final Supplier<ExecutorService> executorServiceSupplier =
        () -> LoggingExecutors.newCachedThreadPool("membership", false);
    Path locatorDirectory = temporaryFolder.newFolder().toPath();

    final MembershipConfig config =
        createMembershipConfig(true, DEFAULT_LOCATOR_WAIT_TIME, memberName, locatorPorts);

    return MembershipLocatorBuilder.<MemberIdentifier>newLocatorBuilder(
        socketCreator,
        dsfidSerializer,
        locatorDirectory,
        executorServiceSupplier)
        .setConfig(config)
        .setPort(localPort)
        .create();
  }

  private void stop(final Membership<MemberIdentifier>... memberships) {
    Arrays.stream(memberships).forEach(membership -> {
      if (membership != null) {
        membership.disconnect(false);
      }
    });
  }

  private void stop(final MembershipLocator<MemberIdentifier>... locators) {
    Arrays.stream(locators).forEach(locator -> locator.stop());
  }
}
