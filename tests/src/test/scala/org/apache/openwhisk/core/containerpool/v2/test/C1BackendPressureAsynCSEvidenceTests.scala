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

import org.apache.openwhisk.core.containerpool.v2.FunctionPullingContainerProxy
import org.junit.runner.RunWith
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.junit.JUnitRunner
import spray.json._

@RunWith(classOf[JUnitRunner])
class C1BackendPressureAsynCSEvidenceTests extends AnyFlatSpec with Matchers {
  private def producerEvent(code: String, boundary: String, unixNs: JsValue, monoNs: JsValue): JsObject =
    JsObject(
      "event_code" -> JsString(code),
      "event_seq" -> JsNumber(code.drop(1).toInt),
      "logical_request_id" -> JsString("7"),
      "attempt_id" -> JsString("1"),
      "process" -> JsString(if (code.startsWith("A2")) "asyncs_adapter" else "asyncs_worker"),
      "pid" -> JsString(if (code.startsWith("A2")) "42" else ""),
      "tid" -> JsString(""),
      "unix_ns" -> unixNs,
      "mono_ns" -> monoNs,
      "clock_domain" -> JsString(if (unixNs == JsString("")) "asyncs_worker_mono" else "asyncs_adapter_wall"),
      "status" -> JsString("observed"),
      "attrs" -> JsObject("boundary" -> JsString(boundary)))

  behavior of "C1 backend-pressure AsynCS evidence bridge"

  it should "bridge only marked requests and actual producer events" in {
    val result = JsObject(
      "C_out" -> JsString("must-not-be-logged"),
      "rid" -> JsString("actual-rid-7"),
      "skU" -> JsString("must-not-be-logged"),
      "encrypted_input" -> JsString("must-not-be-logged"),
      "trace" -> JsObject(
        "worker_started_this_invocation" -> JsBoolean(false),
        "kms_contacted" -> JsBoolean(false),
        "enclave_key_cache_hit" -> JsBoolean(true),
        "function_cache_hit" -> JsBoolean(true),
        "cfunc_decrypt_executed" -> JsBoolean(false),
        "payload_bytes_consumed" -> JsNumber(1024),
        "payload_sha256" -> JsString("payload-sha256"),
        "workload_timing_schema" -> JsString("enclave-rdtsc-v1"),
        "workload_core_cycles" -> JsNumber(12000),
        "output_kem_cycles" -> JsNumber(3000),
        "output_aes_gcm_cycles" -> JsNumber(900),
        "tsc_hz" -> JsNumber(2400000000L),
        "tsc_frequency_method" -> JsString("cpuid_0x15_crystal"),
        "workload_core_duration_ns" -> JsNumber(5000),
        "output_kem_duration_ns" -> JsNumber(1250),
        "output_aes_gcm_duration_ns" -> JsNumber(375),
        "producer_timing_events" -> JsArray(
          producerEvent("A200", "adapter_worker_invoke_enter", JsNumber(1000), JsNumber(100)),
          producerEvent("A210", "adapter_worker_invoke_exit", JsNumber(2000), JsNumber(200)),
          producerEvent("A400", "function_execute_enter", JsNumber(1200), JsNumber(120)),
          producerEvent("A410", "function_execute_exit", JsNumber(1800), JsNumber(180)))))

    val lines = FunctionPullingContainerProxy.c1BackendPressureAsynCSEvidenceLines(
      markedBackendPressure = true,
      result = Some(result),
      activationId = "activation-7",
      node = "WORKER_HOST_0_PLACEHOLDER",
      transactionId = "tid-7")

    lines.head should startWith("C1_BACKEND_PRESSURE_ASYNCS_RESULT|")
    lines.head should include("actual_rid=actual-rid-7")
    lines.head should include("worker_started_this_invocation=false")
    lines.head should include("kms_contacted=false")
    lines.head should include("enclave_key_cache_hit=true")
    lines.head should include("function_cache_hit=true")
    lines.head should include("cfunc_decrypt_executed=false")
    lines.head should include("payload_bytes_consumed=1024")
    lines.head should include("workload_timing_schema=enclave-rdtsc-v1")
    lines.head should include("workload_core_cycles=12000")
    lines.head should include("output_kem_cycles=3000")
    lines.head should include("output_aes_gcm_cycles=900")
    lines.head should include("tsc_hz=2400000000")
    lines.head should include("tsc_frequency_method=cpuid_0x15_crystal")
    lines.head should include("workload_core_duration_ns=5000")
    lines.head should include("output_kem_duration_ns=1250")
    lines.head should include("output_aes_gcm_duration_ns=375")
    lines.tail.map(_.split("event_code=")(1).takeWhile(_ != '|')) shouldBe Seq("A200", "A210", "A400", "A410")
    lines.tail.foreach { line =>
      line should startWith("C1TIMING_EVENT|")
      line should include("activation_id=activation-7")
      line should include("logical_request_id=7")
      line should include("attempt_id=1")
      line should include("node=WORKER_HOST_0_PLACEHOLDER")
    }
    lines.mkString("\n") should not include "A300"
    lines.mkString("\n") should not include "A310"
    lines.mkString("\n") should not include "A320"
    lines.mkString("\n") should not include "A350"
    lines.mkString("\n") should not include "C_out=must-not-be-logged"
    lines.mkString("\n") should not include "must-not-be-logged"
    FunctionPullingContainerProxy
      .c1BackendPressureAsynCSEvidenceLines(
        markedBackendPressure = false,
        result = Some(result),
        activationId = "activation-7",
        node = "WORKER_HOST_0_PLACEHOLDER",
        transactionId = "tid-7") shouldBe empty
  }

  it should "preserve conditional KMS and enclave startup events when present" in {
    val conditionalCodes = Seq("A300", "A310", "A320", "A330", "A340", "A350")
    val events = conditionalCodes.zipWithIndex.map {
      case (code, index) =>
        JsObject(
          "event_code" -> JsString(code),
          "event_seq" -> JsNumber(code.drop(1).toInt),
          "logical_request_id" -> JsString("8"),
          "attempt_id" -> JsString("1"),
          "process" -> JsString("asyncs_worker"),
          "pid" -> JsString(""),
          "tid" -> JsString(""),
          "unix_ns" -> JsString(""),
          "mono_ns" -> JsNumber(300 + index),
          "clock_domain" -> JsString("asyncs_worker_mono"),
          "status" -> JsString("observed"),
          "attrs" -> JsObject("boundary" -> JsString(s"conditional-$code")))
    }
    val result = JsObject(
      "rid" -> JsString("actual-rid-8"),
      "trace" -> JsObject(
        "worker_started_this_invocation" -> JsBoolean(true),
        "kms_contacted" -> JsBoolean(true),
        "producer_timing_events" -> JsArray(events.toVector)))

    val lines = FunctionPullingContainerProxy.c1BackendPressureAsynCSEvidenceLines(
      markedBackendPressure = true,
      result = Some(result),
      activationId = "activation-8",
      node = "WORKER_HOST_1_PLACEHOLDER",
      transactionId = "tid-8")

    lines.tail.map(_.split("event_code=")(1).takeWhile(_ != '|')) shouldBe conditionalCodes
  }
}
