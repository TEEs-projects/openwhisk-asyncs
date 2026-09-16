/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.openwhisk.core.controller.test

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Promise}
import org.junit.runner.RunWith
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.junit.JUnitRunner

import org.apache.openwhisk.core.controller.C1BackendPressureCompletionBeforeDeadline
import org.apache.openwhisk.core.controller.C1BackendPressureCompletionWindowDeadlineReached
import org.apache.openwhisk.core.controller.C1BackendPressurePlateauState
import org.apache.openwhisk.core.controller.C1BackendPressureCompletionWindowTransitions

@RunWith(classOf[JUnitRunner])
class C1BackendPressureCompletionWindowStateTests extends AnyFlatSpec with Matchers {

  private val plateauWindowSize = 200
  private val warmupCompletions = 1000
  private val warmupNs = 10L * 1000000000L
  private val noImproveNs = 10L * 1000000000L
  private val minImprovementFraction = 0.03

  private def completionHistory(completionCount: Int,
                                completionElapsedNs: Long,
                                rollingQps: Double): Vector[Long] = {
    val priorCompletionCount = completionCount - 1
    val windowStartIndex = priorCompletionCount - plateauWindowSize
    val windowElapsedNs = math.round(plateauWindowSize.toDouble * 1000000000.0 / rollingQps)
    val windowStartNs = completionElapsedNs - windowElapsedNs
    Vector.tabulate(priorCompletionCount) { index =>
      if (index <= windowStartIndex) {
        windowStartNs - (windowStartIndex - index).toLong
      } else {
        windowStartNs + (completionElapsedNs - windowStartNs) * (index - windowStartIndex) / plateauWindowSize
      }
    }
  }

  private def evaluate(previous: C1BackendPressurePlateauState,
                       completionCount: Int,
                       completionElapsedNs: Long,
                       rollingQps: Double,
                       plateauEnabled: Boolean = true): C1BackendPressurePlateauState =
    C1BackendPressureCompletionWindowTransitions.evaluatePlateauCompletion(
      previous = previous,
      completionTimesNs = completionHistory(completionCount, completionElapsedNs, rollingQps),
      completionElapsedNs = completionElapsedNs,
      plateauEnabled = plateauEnabled,
      plateauWindowSize = plateauWindowSize,
      plateauWarmupCompletions = warmupCompletions,
      plateauWarmupNs = warmupNs,
      plateauNoImproveNs = noImproveNs,
      plateauMinImprovementFraction = minImprovementFraction)

  behavior of "C1 backend-pressure completion-window transitions"

  it should "bound completion waiting at the monotonic controller deadline" in {
    implicit val executionContext: ExecutionContext = ExecutionContext.global
    val runStartMonoNs = 1000000000L
    val deadlineMonoNs =
      C1BackendPressureCompletionWindowTransitions.completionWindowDeadlineMonoNs(runStartMonoNs, timeoutSec = 7)
    deadlineMonoNs shouldBe 8000000000L

    val completionBeforeDeadline = Promise[String]()
    val pendingDeadline = Promise[Unit]()
    val completed = C1BackendPressureCompletionWindowTransitions.firstCompletionOrDeadline(
      completionBeforeDeadline.future,
      pendingDeadline.future,
      deadlineMonoNs,
      () => deadlineMonoNs - 1L)
    completionBeforeDeadline.success("terminal")
    Await.result(completed, 1.second) shouldBe C1BackendPressureCompletionBeforeDeadline(
      "terminal",
      deadlineMonoNs - 1L)

    val unresolvedCompletion = Promise[String]()
    val reachedDeadline = Promise[Unit]()
    val timedOut = C1BackendPressureCompletionWindowTransitions.firstCompletionOrDeadline(
      unresolvedCompletion.future,
      reachedDeadline.future,
      deadlineMonoNs,
      () => deadlineMonoNs)
    reachedDeadline.success(())
    Await.result(timedOut, 1.second) shouldBe C1BackendPressureCompletionWindowDeadlineReached(deadlineMonoNs)
    unresolvedCompletion.isCompleted shouldBe false

    val lateCompletion = Promise[String]()
    val delayedDeadlineSignal = Promise[Unit]()
    val late = C1BackendPressureCompletionWindowTransitions.firstCompletionOrDeadline(
      lateCompletion.future,
      delayedDeadlineSignal.future,
      deadlineMonoNs,
      () => deadlineMonoNs)
    lateCompletion.success("too-late")
    Await.result(late, 1.second) shouldBe C1BackendPressureCompletionWindowDeadlineReached(deadlineMonoNs)
  }

  it should "block refill at deadline without changing the plateau latch" in {
    val atDeadline = C1BackendPressureCompletionWindowTransitions.afterTerminalCompletion(
      nextLogicalRequestId = 1230,
      remainingInFlight = 199,
      targetLogicalRequests = 100000,
      plateauStopNow = false,
      stopAlreadyLatched = false,
      refillAllowed = false)

    atDeadline.refillLogicalRequestId shouldBe None
    atDeadline.nextLogicalRequestId - 1 shouldBe 1229
    atDeadline.inFlightAfterCompletion shouldBe 199
    atDeadline.stopLatched shouldBe false
  }

  it should "start the no-improve clock only after both warmup gates" in {
    val initial = C1BackendPressurePlateauState(rollingWindowSize = plateauWindowSize)

    val completion598 = evaluate(
      initial,
      completionCount = 598,
      completionElapsedNs = 25202126731L,
      rollingQps = 45.135058553078444)
    completion598.currentRollingQps should be > 0.0
    completion598.bestRollingQps shouldBe 0.0
    completion598.bestCompletionElapsedNs shouldBe 0L
    completion598.stop shouldBe false

    val countWarmButTimeCold = evaluate(
      completion598,
      completionCount = 1000,
      completionElapsedNs = warmupNs - 1L,
      rollingQps = 45.0)
    countWarmButTimeCold.bestRollingQps shouldBe 0.0
    countWarmButTimeCold.bestCompletionElapsedNs shouldBe 0L
    countWarmButTimeCold.stop shouldBe false

    val completion1000 = evaluate(
      countWarmButTimeCold,
      completionCount = 1000,
      completionElapsedNs = 34386441082L,
      rollingQps = 45.002280239663435)
    completion1000.bestRollingQps should be > 45.0
    completion1000.bestCompletionElapsedNs shouldBe 34386441082L
    completion1000.stop shouldBe false

    val completion1030 = evaluate(
      completion1000,
      completionCount = 1030,
      completionElapsedNs = 35205674761L,
      rollingQps = 43.49792944179377)
    completion1030.bestCompletionElapsedNs shouldBe 34386441082L
    completion1030.stop shouldBe false

    val justBeforeFullInterval = evaluate(
      completion1030,
      completionCount = 1200,
      completionElapsedNs = 44386441081L,
      rollingQps = 43.0)
    justBeforeFullInterval.stop shouldBe false

    val fullInterval = evaluate(
      justBeforeFullInterval,
      completionCount = 1201,
      completionElapsedNs = 44386441082L,
      rollingQps = 43.0)
    fullInterval.stop shouldBe true
    fullInterval.stopReason shouldBe "plateau_no_improvement"
  }

  it should "reset the no-improve clock after a qualifying improvement" in {
    val baseline = evaluate(
      C1BackendPressurePlateauState(rollingWindowSize = plateauWindowSize),
      completionCount = 1000,
      completionElapsedNs = 34386441082L,
      rollingQps = 45.0)

    val improved = evaluate(
      baseline,
      completionCount = 1100,
      completionElapsedNs = 40000000000L,
      rollingQps = 47.0)
    improved.bestRollingQps should be > 46.9
    improved.bestCompletionElapsedNs shouldBe 40000000000L
    improved.stop shouldBe false

    val beforeResetInterval = evaluate(
      improved,
      completionCount = 1300,
      completionElapsedNs = 49999999999L,
      rollingQps = 46.0)
    beforeResetInterval.stop shouldBe false

    val afterResetInterval = evaluate(
      beforeResetInterval,
      completionCount = 1301,
      completionElapsedNs = 50000000000L,
      rollingQps = 46.0)
    afterResetInterval.stop shouldBe true
    afterResetInterval.bestCompletionElapsedNs shouldBe 40000000000L
  }

  it should "preserve plateau-disabled behavior" in {
    val previous = C1BackendPressurePlateauState(
      bestRollingQps = 12.0,
      bestCompletionElapsedNs = 7L,
      currentRollingQps = 11.0,
      rollingWindowSize = 17)

    evaluate(
      previous,
      completionCount = 1000,
      completionElapsedNs = 34386441082L,
      rollingQps = 45.0,
      plateauEnabled = false) shouldBe previous.copy(rollingWindowSize = plateauWindowSize)
  }

  it should "latch the first plateau stop while draining later completions" in {
    val targetLogicalRequests = 100000

    val beforeStop = C1BackendPressureCompletionWindowTransitions.afterTerminalCompletion(
      nextLogicalRequestId = 1230,
      remainingInFlight = 199,
      targetLogicalRequests = targetLogicalRequests,
      plateauStopNow = false,
      stopAlreadyLatched = false)

    beforeStop.refillLogicalRequestId shouldBe Some(1230)
    beforeStop.nextLogicalRequestId shouldBe 1231
    beforeStop.nextLogicalRequestId - 1 shouldBe 1230
    beforeStop.inFlightAfterCompletion shouldBe 200
    beforeStop.stopLatched shouldBe false

    val firstStop = C1BackendPressureCompletionWindowTransitions.afterTerminalCompletion(
      nextLogicalRequestId = 1230,
      remainingInFlight = 199,
      targetLogicalRequests = targetLogicalRequests,
      plateauStopNow = true,
      stopAlreadyLatched = false)

    firstStop.refillLogicalRequestId shouldBe None
    firstStop.nextLogicalRequestId - 1 shouldBe 1229
    firstStop.inFlightAfterCompletion shouldBe 199
    firstStop.stopLatched shouldBe true

    val drainedToSeven = (1 to 192).foldLeft(firstStop) { (state, _) =>
      C1BackendPressureCompletionWindowTransitions.afterTerminalCompletion(
        nextLogicalRequestId = state.nextLogicalRequestId,
        remainingInFlight = state.inFlightAfterCompletion - 1,
        targetLogicalRequests = targetLogicalRequests,
        plateauStopNow = true,
        stopAlreadyLatched = state.stopLatched)
    }

    drainedToSeven.nextLogicalRequestId - 1 shouldBe 1229
    drainedToSeven.inFlightAfterCompletion shouldBe 7
    drainedToSeven.refillLogicalRequestId shouldBe None

    val laterQpsImprovement = C1BackendPressureCompletionWindowTransitions.afterTerminalCompletion(
      nextLogicalRequestId = drainedToSeven.nextLogicalRequestId,
      remainingInFlight = drainedToSeven.inFlightAfterCompletion - 1,
      targetLogicalRequests = targetLogicalRequests,
      plateauStopNow = false,
      stopAlreadyLatched = drainedToSeven.stopLatched)

    laterQpsImprovement.nextLogicalRequestId - 1 shouldBe 1229
    laterQpsImprovement.inFlightAfterCompletion shouldBe 6
    laterQpsImprovement.refillLogicalRequestId shouldBe None
    laterQpsImprovement.stopLatched shouldBe true

    val fullyDrained = (1 to 6).foldLeft(laterQpsImprovement) { (state, _) =>
      C1BackendPressureCompletionWindowTransitions.afterTerminalCompletion(
        nextLogicalRequestId = state.nextLogicalRequestId,
        remainingInFlight = state.inFlightAfterCompletion - 1,
        targetLogicalRequests = targetLogicalRequests,
        plateauStopNow = false,
        stopAlreadyLatched = state.stopLatched)
    }

    fullyDrained.nextLogicalRequestId - 1 shouldBe 1229
    fullyDrained.inFlightAfterCompletion shouldBe 0
    fullyDrained.refillLogicalRequestId shouldBe None
    fullyDrained.stopLatched shouldBe true
  }
}
