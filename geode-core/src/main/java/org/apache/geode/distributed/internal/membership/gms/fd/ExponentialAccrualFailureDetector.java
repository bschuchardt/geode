/*
 * Copyright 2018 Mitsunori Komatsu (komamitsu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.geode.distributed.internal.membership.gms.fd;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.Logger;

import org.apache.geode.internal.logging.LogService;

/**
 * Patterned after the Cassandra accrual failure detector this class uses simplified
 * calculations to form Phi
 */
public class ExponentialAccrualFailureDetector implements FailureDetector {
  private static final Logger logger = LogService.getLogger();

  private final double threshold;

  private final HeartbeatHistory heartbeatHistory;
  private final AtomicReference<Long> lastTimestampMillis = new AtomicReference<Long>();
  private final AtomicLong heartbeatsRecorded = new AtomicLong();

  private static final double PHI_MULTIPLIER = 1.0 / Math.log(10.0);

  /**
   * @param threshold A low threshold is prone to generate many wrong suspicions but ensures a quick
   *        detection in the event
   *        of a real crash. Conversely, a high threshold generates fewer mistakes but needs more
   *        time to detect
   *        actual crashes
   *
   * @param maxSampleSize Number of samples to use for calculation of mean and standard deviation of
   *        inter-arrival times.
   * @param firstHeartbeatIntervalMillis Bootstrap the stats with heartbeats that corresponds to
   *        to this duration, with a with rather high standard deviation (since environment is
   *        unknown
   *        in the beginning)
   */
  protected ExponentialAccrualFailureDetector(double threshold, int maxSampleSize,
                                              long firstHeartbeatIntervalMillis) {
    if (threshold <= 0) {
      throw new IllegalArgumentException("Threshold must be > 0: " + threshold);
    }
    if (maxSampleSize <= 0) {
      throw new IllegalArgumentException("Sample size must be > 0: " + maxSampleSize);
    }
    if (firstHeartbeatIntervalMillis <= 0) {
      throw new IllegalArgumentException(
          "First heartbeat value must be > 0: " + firstHeartbeatIntervalMillis);
    }

    this.threshold = threshold;

    heartbeatHistory = new HeartbeatHistory(maxSampleSize);
    heartbeatHistory.add(firstHeartbeatIntervalMillis).add(firstHeartbeatIntervalMillis);
  }

  @Override
  public synchronized double availabilityProbability(long timestampMillis) {
    Long lastTimestampMillis = this.lastTimestampMillis.get();
    if (lastTimestampMillis == null) {
      return 0.0;
    }

    long tDelta = timestampMillis - lastTimestampMillis;
    double mean = heartbeatHistory.mean();
    double phi = (tDelta / mean);
    phi = phi * phi;
    return PHI_MULTIPLIER * phi;
  }

  @Override
  public boolean isAvailable(long timestampMillis) {
    return availabilityProbability(timestampMillis) < threshold;
  }

  public boolean isAvailable() {
    return isAvailable(System.currentTimeMillis());
  }

  @Override
  public long heartbeatsRecorded() {
    return heartbeatsRecorded.get();
  }

  @Override
  public synchronized void heartbeat(long timestampMillis) {
    Long lastTimestampMillis = this.lastTimestampMillis.getAndSet(timestampMillis);
    /** bruce s.: for Apache Geode, don't record duplicate heartbeats */
    if (lastTimestampMillis != null && lastTimestampMillis >= timestampMillis) {
      return;
    }
    if (lastTimestampMillis != null) {
      long interval = timestampMillis - lastTimestampMillis;
      if (isAvailable(timestampMillis)) {
        heartbeatHistory.add(interval);
        heartbeatsRecorded.incrementAndGet();
      }
    }
  }

  @Override
  public Long getLastTimestampMillis() {
    return lastTimestampMillis.get();
  }

  public double getThreshold() {
    return threshold;
  }

  @Override
  public void heartbeat() {
    heartbeat(System.currentTimeMillis());
  }

  @Override
  public List<Long> getIntervalHistory() {
    return heartbeatHistory.intervals;
  }

  private static class HeartbeatHistory {
    private final int maxSampleSize;
    private final LinkedList<Long> intervals = new LinkedList<>();
    private final AtomicLong intervalSum = new AtomicLong();

    public HeartbeatHistory(int maxSampleSize) {
      if (maxSampleSize < 1) {
        throw new IllegalArgumentException("maxSampleSize must be >= 1, got " + maxSampleSize);
      }
      this.maxSampleSize = maxSampleSize;
    }

    public double mean() {
      return ((double)intervalSum.get()) / ((double)intervals.size());
    }

    public HeartbeatHistory add(long interval) {
      if (intervals.size() >= maxSampleSize) {
        Long dropped = intervals.pollFirst();
        intervalSum.addAndGet(-dropped);
      }
      intervals.add(interval);
      intervalSum.addAndGet(interval);
      return this;
    }

  }

}
