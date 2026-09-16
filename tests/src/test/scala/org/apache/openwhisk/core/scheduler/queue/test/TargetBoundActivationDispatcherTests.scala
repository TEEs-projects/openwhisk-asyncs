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

package org.apache.openwhisk.core.scheduler.queue.test

import common.StreamLogging
import java.util.Base64

import org.apache.pekko.util.ByteString
import org.apache.openwhisk.common.TransactionId
import org.apache.openwhisk.core.connector.ActivationMessage
import org.apache.openwhisk.core.entity.ExecManifest.{ImageName, RuntimeManifest}
import org.apache.openwhisk.core.entity._
import org.apache.openwhisk.core.scheduler.grpc.GetActivation
import org.apache.openwhisk.core.scheduler.queue._
import org.junit.runner.RunWith
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.junit.JUnitRunner
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

@RunWith(classOf[JUnitRunner])
class TargetBoundActivationDispatcherTests extends AnyFlatSpec with Matchers with StreamLogging {
  private implicit val ec: ExecutionContext = ExecutionContext.global

  behavior of "GatewayTargetBoundActivationDispatcher"

  it should "return target CODE and INPUT in one cold fetch contract" in {
    val calls = ArrayBuffer.empty[ProtectedEnvelopeV1]
    val gateway = recordingGateway(calls)
    val provider = fixedProvider(sourceCode)
    val dispatcher = new GatewayTargetBoundActivationDispatcher(provider, gateway)

    val result = Await.result(dispatcher.prepare(request(warmed = false), activation), 3.seconds).toOption.get
    calls.map(_.kind) shouldBe Seq(ProtectedObjectKind.Code, ProtectedObjectKind.Input)
    result.activationId shouldBe activation.activationId

    val contract = targetContract(result)
    contract.fields(TargetBoundActivationContent.TargetBindingField) shouldBe JsString("42")
    contract.fields(TargetBoundActivationContent.SourceBindingField) shouldBe JsString("17")
    contract.fields(TargetBoundActivationContent.ExactRevisionField) shouldBe JsString(revision.rev)
    contract.fields(TargetBoundActivationContent.WarmedField) shouldBe JsBoolean(false)
    contract.fields(TargetBoundActivationContent.WorkerCodeFetchField) shouldBe JsBoolean(false)
    decodeTarget(contract, TargetBoundActivationContent.TargetCodeField).kind shouldBe ProtectedObjectKind.Code
    decodeTarget(contract, TargetBoundActivationContent.TargetInputField).kind shouldBe ProtectedObjectKind.Input
    val parsed = TargetBoundActivationContent.parseTargetDispatch(result.content).toOption.get
    parsed.targetBindingId shouldBe 42
    parsed.sourceBindingId shouldBe 17
    parsed.warmed shouldBe false
    parsed.targetCode.map(_.kind) shouldBe Some(ProtectedObjectKind.Code)
    logLines.exists(line => line.contains("event_code=RG270") && line.contains("operation=CODE_INPUT")) shouldBe true
    logLines.exists(line => line.contains("event_code=RG280") && line.contains("status=success")) shouldBe true
  }

  it should "return only target INPUT for a warm pull without loading action code" in {
    var providerCalls = 0
    val provider = new ExactRevisionProtectedCodeProvider {
      override def load(activation: ActivationMessage) = {
        providerCalls += 1
        Future.successful(Right(SourceProtectedCode(revision, sourceCode)))
      }
    }
    val calls = ArrayBuffer.empty[ProtectedEnvelopeV1]
    val dispatcher = new GatewayTargetBoundActivationDispatcher(provider, recordingGateway(calls))

    val result = Await.result(dispatcher.prepare(request(warmed = true), activation), 3.seconds).toOption.get
    providerCalls shouldBe 0
    calls.map(_.kind) shouldBe Seq(ProtectedObjectKind.Input)
    val contract = targetContract(result)
    contract.fields(TargetBoundActivationContent.WarmedField) shouldBe JsBoolean(true)
    contract.fields should not contain TargetBoundActivationContent.TargetCodeField
    decodeTarget(contract, TargetBoundActivationContent.TargetInputField).direction shouldBe
      ProtectedEnvelopeDirection.GatewayToTarget
    logLines.exists(line => line.contains("event_code=RG270") && line.contains("operation=INPUT")) shouldBe true
  }

  it should "leave existing non-target-bound profiles unchanged" in {
    val dispatcher =
      new GatewayTargetBoundActivationDispatcher(fixedProvider(sourceCode), recordingGateway(ArrayBuffer.empty))
    val ordinary = request(warmed = false).copy(targetBindingId = None)

    Await.result(dispatcher.prepare(ordinary, activation), 3.seconds) shouldBe Right(activation)
  }

  it should "reject a warm target-bound fetch without an exact action revision" in {
    val calls = ArrayBuffer.empty[ProtectedEnvelopeV1]
    val dispatcher = new GatewayTargetBoundActivationDispatcher(fixedProvider(sourceCode), recordingGateway(calls))

    val result =
      Await.result(dispatcher.prepare(request(warmed = true), activation.copy(revision = DocRevision.empty)), 3.seconds)
    result.left.get.causedBy should include("exact action revision")
    calls shouldBe empty
  }

  it should "return a sanitized explicit error when Gateway re-encryption fails" in {
    val gateway = new GatewayControlClient {
      override def reencryptToTarget(targetBindingId: Long, sourceEnvelope: ProtectedEnvelopeV1) =
        Future.successful(Left(GatewayControlRejected(77)))
    }
    val dispatcher = new GatewayTargetBoundActivationDispatcher(fixedProvider(sourceCode), gateway)

    val error = Await.result(dispatcher.prepare(request(warmed = false), activation), 3.seconds).left.get
    error.causedBy should include("status=77")
    error.causedBy should not include Base64.getEncoder.encodeToString(sourceInput.bytes.toArray)
  }

  behavior of "WhiskActionExactRevisionProtectedCodeProvider"

  it should "load the exact action revision and extract its source-protected CODE" in {
    var observed: Option[(DocId, DocRevision)] = None
    val code = Base64.getEncoder.encodeToString(sourceCode.bytes.toArray)
    val exec = CodeExecAsString(RuntimeManifest("reusable-concurrency:1", ImageName("runtime")), code, None)
    val stored = WhiskAction(actionPath, actionName, exec).revision[WhiskAction](revision)
    val provider = new WhiskActionExactRevisionProtectedCodeProvider((docId, rev, _) => {
      observed = Some(docId -> rev)
      Future.successful(stored)
    })

    Await.result(provider.load(activation), 3.seconds) shouldBe Right(SourceProtectedCode(revision, sourceCode))
    observed shouldBe Some(activation.action.toDocId -> revision)
  }

  it should "reject an activation that lacks an exact revision without performing a lookup" in {
    var lookedUp = false
    val provider = new WhiskActionExactRevisionProtectedCodeProvider((_, _, _) => {
      lookedUp = true
      Future.failed(new IllegalStateException("must not run"))
    })

    Await.result(provider.load(activation.copy(revision = DocRevision.empty)), 3.seconds).isLeft shouldBe true
    lookedUp shouldBe false
  }

  private def fixedProvider(envelope: ProtectedEnvelopeV1) = new ExactRevisionProtectedCodeProvider {
    override def load(activation: ActivationMessage) =
      Future.successful(Right(SourceProtectedCode(revision, envelope)))
  }

  private def recordingGateway(calls: ArrayBuffer[ProtectedEnvelopeV1]) = new GatewayControlClient {
    override def reencryptToTarget(targetBindingId: Long, sourceEnvelope: ProtectedEnvelopeV1) = {
      calls += sourceEnvelope
      Future.successful(
        Right(sourceEnvelope.copy(direction = ProtectedEnvelopeDirection.GatewayToTarget, bindingId = targetBindingId)))
    }
  }

  private def targetContract(message: ActivationMessage): JsObject =
    message.content.get.asJsObject.fields(TargetBoundActivationContent.RootField).asJsObject

  private def decodeTarget(contract: JsObject, field: String): ProtectedEnvelopeV1 = {
    val encoded = contract.fields(field).convertTo[String]
    ProtectedEnvelopeV1
      .decode(ByteString.fromArray(Base64.getDecoder.decode(encoded)))
      .fold(message => fail(message), envelope => envelope)
  }

  private def request(warmed: Boolean) =
    GetActivation(TransactionId("pull"), actionFqn, "container", warmed, None, targetBindingId = Some(42L))

  private val actionPath = EntityPath("namespace")
  private val actionName = EntityName("action")
  private val actionFqn = FullyQualifiedEntityName(actionPath, actionName, Some(SemVer(1, 0, 0)))
  private val revision = DocRevision("1-exact")
  private val identityUuid = UUID()
  private val identity = Identity(
    Subject(),
    Namespace(EntityName("invocation-namespace"), identityUuid),
    BasicAuthenticationAuthKey(identityUuid, Secret()),
    Set.empty)

  private val sourceInput = ProtectedEnvelopeV1(
    ProtectedObjectKind.Input,
    ProtectedEnvelopeDirection.SourceToGateway,
    bindingId = 17,
    ProtectedCorrelationKind.RequestId,
    ByteString.fromArray((0 until 32).map(_.toByte).toArray),
    ByteString.fromArray((0 until 12).map(_.toByte).toArray),
    ByteString("input"),
    ByteString.fromArray(Array.fill(16)(1.toByte)))

  private val sourceCode = ProtectedEnvelopeV1(
    ProtectedObjectKind.Code,
    ProtectedEnvelopeDirection.SourceToGateway,
    bindingId = 19,
    ProtectedCorrelationKind.CodeObject,
    ByteString.fromArray((32 until 64).map(_.toByte).toArray),
    ByteString.fromArray((16 until 28).map(_.toByte).toArray),
    ByteString("code"),
    ByteString.fromArray(Array.fill(16)(2.toByte)))

  private val sourceContent = JsObject(
    TargetBoundActivationContent.RootField -> JsObject(TargetBoundActivationContent.SourceInputField ->
      JsString(Base64.getEncoder.encodeToString(sourceInput.bytes.toArray))))

  private val activation = ActivationMessage(
    TransactionId(TransactionId.testing.meta.id),
    actionFqn,
    revision,
    identity,
    ActivationId.generate(),
    ControllerInstanceId("0"),
    blocking = true,
    content = Some(sourceContent))
}
