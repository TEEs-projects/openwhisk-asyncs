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

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.security.MessageDigest
import java.util.Base64
import scala.collection.JavaConverters._
import org.apache.pekko.util.ByteString
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.junit.JUnitRunner
import spray.json._
import org.apache.openwhisk.core.controller.{C1BackendPressurePreparedRequest, C1BackendPressurePreparedRequests}
import org.apache.openwhisk.core.entity.ExecManifest.ImageName
import org.apache.openwhisk.core.entity._
import org.apache.openwhisk.core.scheduler.queue.{ProtectedCorrelationKind, ProtectedEnvelopeDirection, ProtectedEnvelopeV1, ProtectedObjectKind, TargetBoundActivationContent}

@RunWith(classOf[JUnitRunner])
class C1BackendPressurePreparedRequestsTests extends AnyFlatSpec with Matchers with BeforeAndAfterEach {
  private var root: Path = _

  override protected def beforeEach(): Unit = {
    root = Files.createTempDirectory("c1-asyncs-prepared-requests-")
  }

  override protected def afterEach(): Unit = {
    if (root != null) {
      Files.walk(root).iterator().asScala.toVector.reverse.foreach(Files.deleteIfExists)
    }
  }

  private def sha256(bytes: Array[Byte]): String =
    MessageDigest.getInstance("SHA-256").digest(bytes).map(byte => f"${byte & 0xff}%02x").mkString

  private def actionMetaData(): WhiskActionMetaData =
    WhiskActionMetaData(
      EntityPath("fixture-namespace/fixture-package"),
      EntityName("asyncs_backend_pressure_fixture"),
      BlackBoxExecMetaData(ImageName("fixture.registry/acsc/openwhisk-runtime:fixture"), None, native = false),
      version = SemVer(1, 2, 3),
      binding = Some(EntityPath("fixture-namespace/source-package")))
      .revision[WhiskActionMetaData](DocRevision("7-fixture-revision"))

  private def writeSource(count: Int = 3): Unit = {
    val rows = (1 to count).map { ordinal =>
      JsObject(
        "action_params" -> JsObject(
          "logical_request_id" -> JsString(ordinal.toString),
          "attempt_id" -> JsString("1"),
          "encrypted_input" -> JsString(s"ciphertext-$ordinal")),
        "expected_rid" -> JsString(s"rid-$ordinal"),
        "logical_request_id" -> JsString(ordinal.toString),
        "ordinal" -> JsNumber(ordinal)).compactPrint
    }
    val requestBytes = (rows.mkString("\n") + "\n").getBytes(StandardCharsets.UTF_8)
    Files.write(root.resolve("requests.jsonl"), requestBytes)
    val manifest = JsObject(
      "action" -> JsObject(
        "requested_action" -> JsString("fixture-provenance-only")),
      "configured_request_count" -> JsNumber(count),
      "failure_probability" -> JsNumber(0),
      "requests_file" -> JsString("requests.jsonl"),
      "requests_sha256" -> JsString(sha256(requestBytes)),
      "schema_version" -> JsString("c1-asyncs-premeasurement-requests-v1"),
      "workload" -> JsObject("workload_id" -> JsString("asyncs-wasm-gzip-level6-mixed-512k")))
    Files.write(root.resolve("manifest.json"), (manifest.prettyPrint + "\n").getBytes(StandardCharsets.UTF_8))
  }

  private def writeReusableSource(): String = {
    val correlation = ByteString.fromArray((0 until 32).map(_.toByte).toArray)
    val expectedRid = correlation.map(byte => f"${byte & 0xff}%02x").mkString
    val envelope = ProtectedEnvelopeV1(
      ProtectedObjectKind.Input,
      ProtectedEnvelopeDirection.SourceToGateway,
      bindingId = 17,
      ProtectedCorrelationKind.RequestId,
      correlation,
      ByteString.fromArray((32 until 44).map(_.toByte).toArray),
      ByteString("protected-input"),
      ByteString.fromArray(Array.fill(16)(1.toByte)))
    val actionParams = JsObject(
      TargetBoundActivationContent.RootField -> JsObject(
        TargetBoundActivationContent.SourceInputField -> JsString(Base64.getEncoder.encodeToString(envelope.bytes.toArray))))
    val row = JsObject(
      "action_params" -> actionParams,
      "expected_rid" -> JsString(expectedRid),
      "logical_request_id" -> JsString("1"),
      "ordinal" -> JsNumber(1)).compactPrint
    val requestBytes = (row + "\n").getBytes(StandardCharsets.UTF_8)
    Files.write(root.resolve("requests.jsonl"), requestBytes)
    val manifest = JsObject(
      "configured_request_count" -> JsNumber(1),
      "failure_probability" -> JsNumber(0),
      "requests_file" -> JsString("requests.jsonl"),
      "requests_sha256" -> JsString(sha256(requestBytes)),
      "schema_version" -> JsString("c1-reusable-concurrency-premeasurement-requests-v1"),
      "workload" -> JsObject("workload_id" -> JsString("reusable-concurrency-sleep77")))
    Files.write(root.resolve("manifest.json"), (manifest.prettyPrint + "\n").getBytes(StandardCharsets.UTF_8))
    expectedRid
  }

  behavior of "C1 backend-pressure prepared request source"

  it should "load sequential action parameters without provider-owned resolved identity" in {
    writeSource()

    val result = C1BackendPressurePreparedRequests.load(
      root.toString,
      expectedCount = 3,
      expectedWorkloadId = "asyncs-wasm-gzip-level6-mixed-512k")

    result.isRight shouldBe true
    val requests = result.right.get
    requests.map(_.ordinal) shouldBe Vector(1, 2, 3)
    requests.map(_.logicalRequestId) shouldBe Vector("1", "2", "3")
    requests.map(_.expectedRid) shouldBe Vector("rid-1", "rid-2", "rid-3")
    requests(1).actionParams.fields("encrypted_input") shouldBe JsString("ciphertext-2")
  }

  it should "derive canonical identity from resolved action metadata" in {
    val action = actionMetaData()
    val identity = C1BackendPressurePreparedRequests.actionIdentity(action)

    identity.canonicalFqen shouldBe JsObject(
      "path" -> JsString("fixture-namespace/fixture-package"),
      "name" -> JsString("asyncs_backend_pressure_fixture"),
      "version" -> JsString("1.2.3"),
      "binding" -> JsString("fixture-namespace/source-package"))
    identity.revision shouldBe "7-fixture-revision"
  }

  it should "load only a target-independent reusable INPUT envelope with matching RID" in {
    val expectedRid = writeReusableSource()

    val result = C1BackendPressurePreparedRequests.load(
      root.toString,
      expectedCount = 1,
      expectedWorkloadId = "reusable-concurrency-sleep77",
      profile = "reusable-concurrency")

    result.isRight shouldBe true
    result.right.get.head.expectedRid shouldBe expectedRid
    result.right.get.head.actionParams.fields.keySet shouldBe Set(TargetBoundActivationContent.RootField)
  }

  it should "describe the real prepared request submit boundary without exposing request payloads" in {
    val request = C1BackendPressurePreparedRequest(
      ordinal = 2,
      logicalRequestId = "2",
      expectedRid = "expected-rid-2",
      actionParams = JsObject("encrypted_input" -> JsString("must-not-be-logged")))

    val fields = C1BackendPressurePreparedRequests.submitEvidenceFields(
      runId = "backend-pressure-asyncs-p0",
      request = request,
      node = "controller0",
      processId = "1234",
      transactionId = "tid-2",
      unixNs = 1000000000L,
      monoNs = 200000L).toMap

    fields("event_code") shouldBe "BP010"
    fields("boundary_name") shouldBe "backend_pressure_request_generated"
    fields("run_id") shouldBe "backend-pressure-asyncs-p0"
    fields("logical_request_id") shouldBe "2"
    fields("attempt_id") shouldBe "1"
    fields("expected_rid") shouldBe "expected-rid-2"
    fields("node") shouldBe "controller0"
    fields("process") shouldBe "backend_pressure_controller"
    fields("pid") shouldBe "1234"
    fields("tid") shouldBe "tid-2"
    fields("unix_ns") shouldBe "1000000000"
    fields("mono_ns") shouldBe "200000"
    fields.values.mkString("|") should not include "must-not-be-logged"
  }

  it should "reject reordered rows even when their file hash matches the manifest" in {
    writeSource()
    val requestsPath = root.resolve("requests.jsonl")
    val reordered = Files.readAllLines(requestsPath, StandardCharsets.UTF_8).asScala.reverse.mkString("\n") + "\n"
    Files.write(requestsPath, reordered.getBytes(StandardCharsets.UTF_8))
    val manifestPath = root.resolve("manifest.json")
    val manifest = new String(Files.readAllBytes(manifestPath), StandardCharsets.UTF_8).parseJson.asJsObject
    val updated = JsObject(manifest.fields + ("requests_sha256" -> JsString(sha256(reordered.getBytes(StandardCharsets.UTF_8)))))
    Files.write(manifestPath, (updated.prettyPrint + "\n").getBytes(StandardCharsets.UTF_8))

    val result = C1BackendPressurePreparedRequests.load(
      root.toString,
      expectedCount = 3,
      expectedWorkloadId = "asyncs-wasm-gzip-level6-mixed-512k")

    result.left.get should include("out of order")
  }
}
