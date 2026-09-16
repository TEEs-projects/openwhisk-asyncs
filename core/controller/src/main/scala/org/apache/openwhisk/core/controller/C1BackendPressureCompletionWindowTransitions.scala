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

package org.apache.openwhisk.core.controller

import scala.concurrent.{ExecutionContext, Future}

private[controller] final case class C1BackendPressureCompletionWindowTransition(
  nextLogicalRequestId: Int,
  inFlightAfterCompletion: Int,
  refillLogicalRequestId: Option[Int],
  stopLatched: Boolean)

private[controller] sealed trait C1BackendPressureCompletionWindowWaitResult[+T]

private[controller] final case class C1BackendPressureCompletionBeforeDeadline[T](value: T, observedMonoNs: Long)
    extends C1BackendPressureCompletionWindowWaitResult[T]

private[controller] final case class C1BackendPressureCompletionWindowDeadlineReached(observedMonoNs: Long)
    extends C1BackendPressureCompletionWindowWaitResult[Nothing]

private[controller] final case class C1BackendPressurePlateauState(bestRollingQps: Double = 0.0,
                                                                   bestCompletionElapsedNs: Long = 0L,
                                                                   currentRollingQps: Double = 0.0,
                                                                   rollingWindowSize: Int = 0,
                                                                   stop: Boolean = false,
                                                                   stopReason: String = "")

private[controller] object C1BackendPressureCompletionWindowTransitions {
  def completionWindowDeadlineMonoNs(runStartMonoNs: Long, timeoutSec: Int): Long =
    runStartMonoNs + timeoutSec.toLong * 1000000000L

  def deadlineReached(deadlineMonoNs: Long, observedMonoNs: Long): Boolean = observedMonoNs >= deadlineMonoNs

  def remainingDeadlineNs(deadlineMonoNs: Long, observedMonoNs: Long): Long =
    math.max(0L, deadlineMonoNs - observedMonoNs)

  def firstCompletionOrDeadline[T](completion: Future[T],
                                   deadline: Future[Unit],
                                   deadlineMonoNs: Long,
                                   nowMonoNs: () => Long)(
    implicit executionContext: ExecutionContext): Future[C1BackendPressureCompletionWindowWaitResult[T]] = {
    val completionResult: Future[C1BackendPressureCompletionWindowWaitResult[T]] = completion.map { value =>
      val observedMonoNs = nowMonoNs()
      if (deadlineReached(deadlineMonoNs, observedMonoNs)) {
        C1BackendPressureCompletionWindowDeadlineReached(observedMonoNs)
      } else {
        C1BackendPressureCompletionBeforeDeadline(value, observedMonoNs)
      }
    }
    val deadlineResult: Future[C1BackendPressureCompletionWindowWaitResult[T]] = deadline.map { _ =>
      C1BackendPressureCompletionWindowDeadlineReached(math.max(deadlineMonoNs, nowMonoNs()))
    }
    Future.firstCompletedOf(Seq(completionResult, deadlineResult))
  }

  def evaluatePlateauCompletion(previous: C1BackendPressurePlateauState,
                                completionTimesNs: Vector[Long],
                                completionElapsedNs: Long,
                                plateauEnabled: Boolean,
                                plateauWindowSize: Int,
                                plateauWarmupCompletions: Int,
                                plateauWarmupNs: Long,
                                plateauNoImproveNs: Long,
                                plateauMinImprovementFraction: Double): C1BackendPressurePlateauState = {
    if (!plateauEnabled || plateauWindowSize <= 0 || completionTimesNs.size < plateauWindowSize) {
      previous.copy(rollingWindowSize = plateauWindowSize)
    } else {
      val windowStartNs = completionTimesNs(completionTimesNs.size - plateauWindowSize)
      val windowElapsedNs = math.max(1L, completionElapsedNs - windowStartNs)
      val rollingQps = plateauWindowSize.toDouble * 1000000000.0 / windowElapsedNs.toDouble
      val reportingState = previous.copy(currentRollingQps = rollingQps, rollingWindowSize = plateauWindowSize)
      val warmupDone =
        completionTimesNs.size + 1 >= plateauWarmupCompletions && completionElapsedNs >= plateauWarmupNs

      if (!warmupDone) {
        reportingState
      } else if (previous.bestRollingQps <= 0.0) {
        C1BackendPressurePlateauState(
          bestRollingQps = rollingQps,
          bestCompletionElapsedNs = completionElapsedNs,
          currentRollingQps = rollingQps,
          rollingWindowSize = plateauWindowSize)
      } else if (rollingQps > previous.bestRollingQps * (1.0 + plateauMinImprovementFraction)) {
        C1BackendPressurePlateauState(
          bestRollingQps = rollingQps,
          bestCompletionElapsedNs = completionElapsedNs,
          currentRollingQps = rollingQps,
          rollingWindowSize = plateauWindowSize)
      } else if (completionElapsedNs - previous.bestCompletionElapsedNs >= plateauNoImproveNs) {
        reportingState.copy(stop = true, stopReason = "plateau_no_improvement")
      } else {
        reportingState
      }
    }
  }

  def afterTerminalCompletion(nextLogicalRequestId: Int,
                              remainingInFlight: Int,
                              targetLogicalRequests: Int,
                              plateauStopNow: Boolean,
                              stopAlreadyLatched: Boolean,
                              refillAllowed: Boolean = true): C1BackendPressureCompletionWindowTransition = {
    val stopLatched = stopAlreadyLatched || plateauStopNow
    if (!stopLatched && refillAllowed && nextLogicalRequestId <= targetLogicalRequests) {
      C1BackendPressureCompletionWindowTransition(
        nextLogicalRequestId = nextLogicalRequestId + 1,
        inFlightAfterCompletion = remainingInFlight + 1,
        refillLogicalRequestId = Some(nextLogicalRequestId),
        stopLatched = false)
    } else {
      C1BackendPressureCompletionWindowTransition(
        nextLogicalRequestId = nextLogicalRequestId,
        inFlightAfterCompletion = remainingInFlight,
        refillLogicalRequestId = None,
        stopLatched = stopLatched)
    }
  }
}
