/*
 * Copyright (C) 2016 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.vortex.runtime.master;

import edu.snu.vortex.runtime.common.plan.physical.TaskGroup;
import edu.snu.vortex.runtime.master.scheduler.RoundRobinScheduler;
import edu.snu.vortex.runtime.master.scheduler.SchedulingPolicy;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;


import static org.junit.Assert.assertEquals;

/**
 * Tests {@link RoundRobinScheduler}
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(TaskGroup.class)
public final class RRSchedulerTest {
  private SchedulingPolicy schedulingPolicy;

  @Before
  public void setUp() {
    schedulingPolicy = new RoundRobinScheduler(2000);
  }

  @Test
  public void checkScheduleTimeout() {
    assertEquals(schedulingPolicy.getScheduleTimeout(), 2000);
  }

  @Test
  public void testTwoTypesOfExecutors() {

  }
}
