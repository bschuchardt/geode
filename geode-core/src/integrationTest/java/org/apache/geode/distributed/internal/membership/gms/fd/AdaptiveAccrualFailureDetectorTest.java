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

import static org.junit.Assert.assertFalse;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.MembershipTest;

@Category({MembershipTest.class})
public class AdaptiveAccrualFailureDetectorTest {
  @Test
  public void isAvailableTest() throws Exception {
    double threshold = 0.8;
    int historySize = 200;
    long now = System.currentTimeMillis();
    long[] intervals =
        new long[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, -14, -15};
    int heartbeatInterval = 5000;
    AdaptiveAccrualFailureDetector detector =
        new AdaptiveAccrualFailureDetector(threshold, historySize, heartbeatInterval);
    long heartbeatTime = 0;
    for (int i=0; i<historySize; i++) {
      now += heartbeatInterval;
      detector.heartbeat(now);
    }
    for (long l : intervals) {
      if (l > 0) {
        detector.heartbeat(now + (heartbeatInterval * l));
      }
      l = Math.abs(l);
      heartbeatTime = now + (heartbeatInterval * l);
      System.out.printf("heartbeat %d probability= %s%n", l,
          detector.availabilityProbability(heartbeatTime));
    }
    assertFalse("probability=" + detector.availabilityProbability(heartbeatTime),
        detector.isAvailable(heartbeatTime));
  }

}
