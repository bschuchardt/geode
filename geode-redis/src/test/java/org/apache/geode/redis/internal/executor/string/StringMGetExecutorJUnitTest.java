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

package org.apache.geode.redis.internal.executor.string;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;

import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.redis.internal.RegionProvider;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class StringMGetExecutorJUnitTest {

  private ExecutionHandlerContext context;
  private MGetExecutor executor;

  @Before
  public void setup() {
    context = mock(ExecutionHandlerContext.class);

    RegionProvider regionProvider = mock(RegionProvider.class);
    when(context.getRegionProvider()).thenReturn(regionProvider);

    when(context.getByteBufAllocator()).thenReturn(new UnpooledByteBufAllocator(false));

    executor = new MGetExecutor();
  }

  @Test
  public void testTooFewOptions() {
    List<byte[]> args = Arrays.asList("MGET".getBytes());
    Command command = new Command(args);
    RedisResponse response = executor.executeCommandWithResponse(command, context);

    assertThat(response.toString())
        .startsWith("-ERR The wrong number of arguments or syntax was provided");
  }

  @Test
  public void testWrongNumberOfOptions() {
    List<byte[]> args = Arrays.asList("MGET".getBytes());

    Command command = new Command(args);
    RedisResponse response = executor.executeCommandWithResponse(command, context);

    assertThat(response.toString())
        .startsWith("-ERR The wrong number of arguments or syntax was provided");
  }
}