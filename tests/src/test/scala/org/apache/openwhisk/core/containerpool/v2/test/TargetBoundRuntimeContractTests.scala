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

package org.apache.openwhisk.core.containerpool.v2.test

import java.util.Base64

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.testkit.TestKit
import org.apache.pekko.util.ByteString
import com.typesafe.config.ConfigFactory
import org.apache.openwhisk.core.containerpool.{ContainerAddress, ContainerId}
import org.apache.openwhisk.core.containerpool.v2._
import org.apache.openwhisk.core.entity.{ActivationResponse, DocRevision}
import org.apache.openwhisk.core.scheduler.queue._
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.junit.JUnitRunner
import spray.json._

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.collection.mutable.ArrayBuffer

@RunWith(classOf[JUnitRunner])
class TargetBoundRuntimeContractTests
    extends TestKit(
      ActorSystem(
        "TargetBoundRuntimeContract",
        ConfigFactory.parseString("pekko.actor.provider=local").withFallback(ConfigFactory.load())))
    with AnyFlatSpecLike
    with Matchers
    with BeforeAndAfterAll {

  implicit private val ec = system.dispatcher

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  behavior of "HttpTargetEndpointBridgeClient"

  it should "parse a matching container-specific endpoint" in {
    val binding = HttpTargetEndpointBridgeClient.parseBindingResponse(
      "container-1",
      """{"status":"ready","binding":{"containerIdentity":"container-1","endpointHost":"WORKER_HOST_0_PLACEHOLDER","endpointPort":9201,"targetHost":"TARGET_HOST_PLACEHOLDER","targetPort":9200}}""")

    binding shouldBe TargetEndpointBinding("container-1", "WORKER_HOST_0_PLACEHOLDER", 9201)
  }

  it should "reject a mismatched container identity or invalid endpoint" in {
    intercept[IllegalArgumentException] {
      HttpTargetEndpointBridgeClient.parseBindingResponse(
        "container-1",
        """{"status":"ready","binding":{"containerIdentity":"container-2","endpointHost":"WORKER_HOST_0_PLACEHOLDER","endpointPort":9201}}""")
    }
    intercept[IllegalArgumentException] {
      HttpTargetEndpointBridgeClient.parseBindingResponse(
        "container-1",
        """{"status":"ready","binding":{"containerIdentity":"container-1","endpointHost":"","endpointPort":9201}}""")
    }
    intercept[IllegalArgumentException] {
      HttpTargetEndpointBridgeClient.parseBindingResponse(
        "container-1",
        """{"status":"ready","binding":{"containerIdentity":"container-1","endpointHost":"WORKER_HOST_0_PLACEHOLDER","endpointPort":0}}""")
    }
  }

  behavior of "GatewayTargetBindingProvider"

  it should "register ACTIVE, write the binding into the same executor, and close it" in {
    @volatile var activated = Option.empty[Long]
    val lifecycle = ArrayBuffer.empty[String]
    val bridge = new TargetEndpointBridgeClient {
      override def bind(containerIdentity: String, targetHost: String, targetPort: Int) = {
        containerIdentity shouldBe "container-1"
        targetHost shouldBe "TARGET_HOST_PLACEHOLDER"
        targetPort shouldBe 9200
        lifecycle += "bridge-put"
        Future.successful(TargetEndpointBinding(containerIdentity, "WORKER_HOST_0_PLACEHOLDER", 9201))
      }
      override def unbind(containerIdentity: String) = {
        containerIdentity shouldBe "container-1"
        lifecycle += "bridge-delete"
        Future.successful(())
      }
    }
    val gateway = new GatewayControlClient {
      override def registerTarget(containerIdentity: String, endpointHost: String, endpointPort: Int) = {
        lifecycle += "gateway-register"
        containerIdentity shouldBe "container-1"
        endpointHost shouldBe "WORKER_HOST_0_PLACEHOLDER"
        endpointPort shouldBe 9201
        Future.successful(Right(GatewayRegisteredTarget(41, 7)))
      }
      override def closeTarget(targetBindingId: Long) = {
        targetBindingId shouldBe 41
        lifecycle += "gateway-close"
        Future.successful(Right(()))
      }
      override def reencryptToTarget(targetBindingId: Long, sourceEnvelope: ProtectedEnvelopeV1) =
        Future.successful(Left(GatewayControlProtocolError("unused")))
    }
    val provider =
      new GatewayTargetBindingProvider(gateway, bridge, 9200, 2.seconds, 10.millis)
    val target = TargetContainer(
      ContainerId("container-1"),
      ContainerAddress("TARGET_HOST_PLACEHOLDER"),
      TargetBindingProvider.ReusableConcurrencyKind,
      (bindingId, _) => {
        lifecycle += "executor-target"
        activated = Some(bindingId)
        Future.successful(())
      })

    val binding = Await.result(provider.awaitReady(target), 3.seconds)
    binding.id shouldBe 41
    activated shouldBe Some(41)
    Await.result(provider.closeBinding(target, binding), 3.seconds)
    lifecycle shouldBe Seq("bridge-put", "gateway-register", "executor-target", "gateway-close", "bridge-delete")
  }

  it should "close a Gateway registration when the executor rejects /target" in {
    val lifecycle = ArrayBuffer.empty[String]
    val bridge = new TargetEndpointBridgeClient {
      override def bind(containerIdentity: String, targetHost: String, targetPort: Int) = {
        lifecycle += "bridge-put"
        Future.successful(TargetEndpointBinding(containerIdentity, "WORKER_HOST_0_PLACEHOLDER", 9202))
      }
      override def unbind(containerIdentity: String) = {
        lifecycle += "bridge-delete"
        Future.successful(())
      }
    }
    val gateway = new GatewayControlClient {
      override def registerTarget(containerIdentity: String, endpointHost: String, endpointPort: Int) = {
        lifecycle += "gateway-register"
        endpointHost shouldBe "WORKER_HOST_0_PLACEHOLDER"
        endpointPort shouldBe 9202
        Future.successful(Right(GatewayRegisteredTarget(42, 8)))
      }
      override def closeTarget(targetBindingId: Long) = {
        lifecycle += "gateway-close"
        Future.successful(Right(()))
      }
      override def reencryptToTarget(targetBindingId: Long, sourceEnvelope: ProtectedEnvelopeV1) =
        Future.successful(Left(GatewayControlProtocolError("unused")))
    }
    val provider =
      new GatewayTargetBindingProvider(gateway, bridge, 9200, 2.seconds, 10.millis)
    val target = TargetContainer(
      ContainerId("container-2"),
      ContainerAddress("TARGET_HOST_2_PLACEHOLDER"),
      TargetBindingProvider.ReusableConcurrencyKind,
      (_, _) => {
        lifecycle += "executor-target"
        Future.failed(new IllegalStateException("executor rejected target"))
      })

    intercept[IllegalStateException](Await.result(provider.awaitReady(target), 3.seconds))
    lifecycle shouldBe Seq("bridge-put", "gateway-register", "executor-target", "gateway-close", "bridge-delete")
  }

  behavior of "FunctionPullingContainerProxy target-bound result adapter"

  it should "preserve source binding and T2G request correlation in the full ack result" in {
    val dispatch = TargetBoundActivationContent.TargetDispatch(
      targetBindingId = 41,
      sourceBindingId = 17,
      DocRevision("1-exact"),
      warmed = true,
      targetInput,
      targetCode = None)
    val response = ActivationResponse.success(
      Some(
        JsObject(
          "status" -> JsString("success"),
          "status_code" -> JsNumber(0),
          "success" -> JsBoolean(true),
          "result" -> JsObject(
            "__reusable_protected_result_envelope" -> JsString(
              Base64.getEncoder.encodeToString(targetResult.bytes.toArray)),
            "__reusable_protected_result_len" -> JsNumber(64),
            "__reusable_request_id_hash" -> JsString(requestHash),
            "__reusable_target_binding_id" -> JsNumber(41)))))

    val adapted = FunctionPullingContainerProxy.targetBoundRuntimeResponse(dispatch, response)
    adapted.isSuccess shouldBe true
    val result = TargetBoundActivationContent.parseTargetResult(adapted.result).toOption.get
    result.sourceBindingId shouldBe 17
    result.targetBindingId shouldBe 41
    result.requestIdHash shouldBe requestHash
    result.targetEnvelope shouldBe targetResult
  }

  behavior of "reusable-concurrency activation store policy"

  it should "skip the store callback only for an enabled reusable-concurrency profile" in {
    val enabled = FunctionPullingContainerProxy.reusableConcurrencyStoreSkipEnabled(
      Map(FunctionPullingContainerProxy.ReusableConcurrencySkipActivationStoreEnv -> "true"))
    val skipReason = FunctionPullingContainerProxy.reusableConcurrencyStoreSkipReason(
      enabled,
      TargetBindingProvider.ReusableConcurrencyKind)
    @volatile var storeCalled = false

    Await.result(
      FunctionPullingContainerProxy.storeActivationUnlessSkipped(skipReason) {
        storeCalled = true
        Future.successful(())
      },
      3.seconds)

    skipReason shouldBe Some(FunctionPullingContainerProxy.ReusableConcurrencyStoreSkipReason)
    storeCalled shouldBe false
  }

  it should "retain the store callback for ordinary actions and a disabled profile gate" in {
    val ordinaryReason = FunctionPullingContainerProxy.reusableConcurrencyStoreSkipReason(
      enabled = true,
      actionKind = "nodejs:20")
    val disabledReason = FunctionPullingContainerProxy.reusableConcurrencyStoreSkipReason(
      enabled = false,
      actionKind = TargetBindingProvider.ReusableConcurrencyKind)
    @volatile var ordinaryStoreCalls = 0

    Await.result(
      FunctionPullingContainerProxy.storeActivationUnlessSkipped(ordinaryReason) {
        ordinaryStoreCalls += 1
        Future.successful(())
      },
      3.seconds)

    ordinaryReason shouldBe None
    disabledReason shouldBe None
    ordinaryStoreCalls shouldBe 1
  }

  private val correlation = ByteString.fromArray((0 until 32).map(_.toByte).toArray)
  private val requestHash = correlation.map(byte => f"${byte & 0xff}%02x").mkString
  private val targetInput = ProtectedEnvelopeV1(
    ProtectedObjectKind.Input,
    ProtectedEnvelopeDirection.GatewayToTarget,
    41,
    ProtectedCorrelationKind.RequestId,
    correlation,
    ByteString.fromArray((0 until 12).map(_.toByte).toArray),
    ByteString("input"),
    ByteString.fromArray(Array.fill(16)(1.toByte)))
  private val targetResult = ProtectedEnvelopeV1(
    ProtectedObjectKind.Result,
    ProtectedEnvelopeDirection.TargetToGateway,
    41,
    ProtectedCorrelationKind.RequestId,
    correlation,
    ByteString.fromArray((128 until 140).map(_.toByte).toArray),
    ByteString("result"),
    ByteString.fromArray(Array.fill(16)(2.toByte)))
}
