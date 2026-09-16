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

package org.apache.openwhisk.core.loadBalancer.test

import common.StreamLogging
import java.time.Instant
import java.util.Base64

import org.apache.pekko.util.ByteString
import org.apache.openwhisk.core.entity._
import org.apache.openwhisk.core.loadBalancer.{GatewayResultFinalizer, P1GatewayResultFinalizer}
import org.apache.openwhisk.core.scheduler.queue._
import org.junit.runner.RunWith
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.junit.JUnitRunner
import spray.json._

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

@RunWith(classOf[JUnitRunner])
class GatewayResultFinalizerTests extends AnyFlatSpec with Matchers with StreamLogging {
  private implicit val ec: ExecutionContext = ExecutionContext.global

  behavior of "P1GatewayResultFinalizer"

  it should "replace a T2G full-ack result with the correlated G2S client envelope" in {
    val gateway = new GatewayControlClient {
      override def reencryptToTarget(targetBindingId: Long, sourceEnvelope: ProtectedEnvelopeV1) =
        Future.successful(Left(GatewayControlProtocolError("unused")))
      override def reencryptResultToSource(
        sourceBindingId: Long,
        targetBindingId: Long,
        targetEnvelope: ProtectedEnvelopeV1) = {
        sourceBindingId shouldBe 17
        targetBindingId shouldBe 41
        targetEnvelope shouldBe t2g
        Future.successful(Right(g2s))
      }
    }
    val finalizer = new P1GatewayResultFinalizer(gateway)

    val result = Await.result(finalizer.finalizeResult(activation), 3.seconds).toOption.get
    result.response.isSuccess shouldBe true
    val fields = result.response.result.get.asJsObject.fields
    fields(GatewayResultFinalizer.RequestIdHashField) shouldBe JsString(requestHash)
    fields(GatewayResultFinalizer.SourceBindingField) shouldBe JsString("17")
    fields(GatewayResultFinalizer.ClientEnvelopeField) shouldBe
      JsString(Base64.getEncoder.encodeToString(g2s.bytes.toArray))
    fields should not contain TargetBoundActivationContent.ResultRootField
    logLines.exists(line => line.contains("event_code=RG810") && line.contains("operation=RESULT")) shouldBe true
    logLines.exists(line => line.contains("event_code=RG820") && line.contains("status=success")) shouldBe true
  }

  it should "return an explicit failure without exposing the target envelope" in {
    val gateway = new GatewayControlClient {
      override def reencryptToTarget(targetBindingId: Long, sourceEnvelope: ProtectedEnvelopeV1) =
        Future.successful(Left(GatewayControlProtocolError("unused")))
      override def reencryptResultToSource(
        sourceBindingId: Long,
        targetBindingId: Long,
        targetEnvelope: ProtectedEnvelopeV1) =
        Future.successful(Left(GatewayControlRejected(23)))
    }
    val finalizer = new P1GatewayResultFinalizer(gateway)

    val failure = Await.result(finalizer.finalizeResult(activation), 3.seconds).left.get
    failure should include("status=23")
    failure should not include Base64.getEncoder.encodeToString(t2g.bytes.toArray)
    GatewayResultFinalizer.systemError(activation, failure).response.isWhiskError shouldBe true
  }

  private val correlation = ByteString.fromArray((0 until 32).map(_.toByte).toArray)
  private val requestHash = correlation.map(byte => f"${byte & 0xff}%02x").mkString
  private val t2g = ProtectedEnvelopeV1(
    ProtectedObjectKind.Result,
    ProtectedEnvelopeDirection.TargetToGateway,
    41,
    ProtectedCorrelationKind.RequestId,
    correlation,
    ByteString.fromArray((128 until 140).map(_.toByte).toArray),
    ByteString("target-result"),
    ByteString.fromArray(Array.fill(16)(1.toByte)))
  private val g2s = t2g.copy(
    direction = ProtectedEnvelopeDirection.GatewayToSource,
    bindingId = 17,
    iv = ByteString.fromArray((144 until 156).map(_.toByte).toArray))
  private val targetResult = TargetBoundActivationContent.targetResult(
    TargetBoundActivationContent.TargetResult(17, 41, requestHash, t2g))
  private val activation = WhiskActivation(
    EntityPath("namespace"),
    EntityName("action"),
    Subject(),
    ActivationId.generate(),
    Instant.now(),
    Instant.now(),
    response = ActivationResponse.success(Some(targetResult)))
}
