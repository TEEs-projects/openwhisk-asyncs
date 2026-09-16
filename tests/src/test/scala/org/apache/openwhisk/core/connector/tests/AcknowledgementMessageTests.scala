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

package org.apache.openwhisk.core.connector.tests

import org.junit.runner.RunWith
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.junit.JUnitRunner
import spray.json._
import org.apache.openwhisk.common.{TransactionId, WhiskInstants}
import org.apache.openwhisk.core.connector.{
  AcknowledgementMessage,
  CombinedCompletionAndResultMessage,
  CompletionMessage,
  ResultMessage
}
import org.apache.openwhisk.core.entity._
import org.apache.openwhisk.core.entity.size.SizeInt

import scala.concurrent.duration.DurationInt
import scala.util.Success

/**
 * Unit tests for the AcknowledgementMessageTests object.
 */
@RunWith(classOf[JUnitRunner])
class AcknowledgementMessageTests extends AnyFlatSpec with Matchers with WhiskInstants {

  behavior of "acknowledgement message"

  val defaultUserMemory: ByteSize = 1024.MB
  val activation = WhiskActivation(
    namespace = EntityPath("ns"),
    name = EntityName("a"),
    Subject(),
    activationId = ActivationId.generate(),
    start = nowInMillis(),
    end = nowInMillis(),
    response = ActivationResponse.success(Some(JsObject("res" -> JsNumber(1)))),
    annotations = Parameters("limits", ActionLimits(TimeLimit(1.second), MemoryLimit(128.MB), LogLimit(1.MB)).toJson),
    duration = Some(123))

  it should "serialize and deserialize a Result message with Left result" in {
    val m = ResultMessage(TransactionId.testing, activation).shrink
    m.response shouldBe 'left
    m.isSlotFree shouldBe empty
    m.serialize shouldBe JsObject("transid" -> m.transid.toJson, "response" -> m.response.left.get.toJson).compactPrint
    m.serialize shouldBe m.toJson.compactPrint
    AcknowledgementMessage.parse(m.serialize) shouldBe Success(m)
  }

  it should "serialize and deserialize a Result message with Right result" in {
    val m = ResultMessage(TransactionId.testing, activation)
    m.response shouldBe 'right
    m.isSlotFree shouldBe empty
    m.serialize shouldBe JsObject("transid" -> m.transid.toJson, "response" -> m.response.right.get.toJson).compactPrint
    AcknowledgementMessage.parse(m.serialize) shouldBe Success(m)
  }

  it should "serialize and deserialize a Completion message" in {
    val m = CompletionMessage(
      TransactionId.testing,
      ActivationId.generate(),
      Some(false),
      InvokerInstanceId(0, userMemory = defaultUserMemory))
    m.isSlotFree should not be empty
    m.serialize shouldBe m.toJson.compactPrint
    AcknowledgementMessage.parse(m.serialize) shouldBe Success(m)
  }

  it should "serialize and deserialize a CombinedCompletionAndResultMessage" in {
    withClue("system error false and right") {
      val c = CombinedCompletionAndResultMessage(
        TransactionId.testing,
        activation,
        InvokerInstanceId(0, userMemory = defaultUserMemory))
      c.response shouldBe 'right
      c.isSlotFree should not be empty
      c.isSystemError shouldBe Some(false)
      c.serialize shouldBe c.toJson.compactPrint
      AcknowledgementMessage.parse(c.serialize) shouldBe Success(c)
    }

    withClue("system error true and right") {
      val response = ActivationResponse.whiskError(JsString("error"))
      val someActivation = activation.copy(response = response)
      val c = CombinedCompletionAndResultMessage(
        TransactionId.testing,
        someActivation,
        InvokerInstanceId(0, userMemory = defaultUserMemory))
      c.response shouldBe 'right
      c.isSlotFree should not be empty
      c.isSystemError shouldBe Some(true)
      c.serialize shouldBe c.toJson.compactPrint
      AcknowledgementMessage.parse(c.serialize) shouldBe Success(c)
    }

    withClue("system error false and left") {
      val c = CombinedCompletionAndResultMessage(
        TransactionId.testing,
        activation,
        InvokerInstanceId(0, userMemory = defaultUserMemory)).shrink
      c.response shouldBe 'left
      c.isSlotFree should not be empty
      c.isSystemError shouldBe Some(false)
      c.serialize shouldBe c.toJson.compactPrint
      AcknowledgementMessage.parse(c.serialize) shouldBe Success(c)
    }

    withClue("system error true and left") {
      val response = ActivationResponse.whiskError(JsString("error"))
      val someActivation = activation.copy(response = response)
      val c = CombinedCompletionAndResultMessage(
        TransactionId.testing,
        someActivation,
        InvokerInstanceId(0, userMemory = defaultUserMemory)).shrink
      c.response shouldBe 'left
      c.isSlotFree should not be empty
      c.isSystemError shouldBe Some(true)
      c.serialize shouldBe c.toJson.compactPrint
      AcknowledgementMessage.parse(c.serialize) shouldBe Success(c)
    }
  }

  it should "deserialize scheduler fallback system-error completion result from 207" in {
    val raw =
      """{"instance":{"asString":"0"},"isSystemError":true,"response":{"activationId":"3bb82da0534f441fb82da0534ff41f9f","annotations":[{"key":"path","value":"guest/c1_backend_pressure_native_hash_207_ccompletion-window"},{"key":"kind","value":"unknown"}],"duration":0,"end":1783600834930,"logs":[],"name":"c1_backend_pressure_native_hash_207_ccompletion-window","namespace":"guest","publish":false,"response":{"result":{"error":"Unexpected http response code: 403 Forbidden (details: {\"error\":\"forbidden\",\"reason\":\"You are not allowed to access this db.\"}\n)"},"statusCode":3},"start":1783600834930,"subject":"guest","version":"0.0.1"},"transid":["oC8O9TMSgKbN4vngKYy3BvTENY0iZNmL",1783600834454,["t70qtIvK7vTzVY6MkjKbIHFYElYB8kBB",1783600834241]]}"""

    val parsed = AcknowledgementMessage.parse(raw)
    parsed.isSuccess shouldBe true
    val message = parsed.get
    message shouldBe a[CombinedCompletionAndResultMessage]
    message.isSlotFree shouldBe Some(SchedulerInstanceId("0"))
    message.isSystemError shouldBe Some(true)
    message.result should not be empty
    val response = message.result.get
    response shouldBe 'right
    response.right.get.activationId.asString shouldBe "3bb82da0534f441fb82da0534ff41f9f"
    response.right.get.response.statusCode shouldBe 3
  }
}
