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

import java.nio.charset.StandardCharsets
import java.security.MessageDigest

import javax.crypto.Cipher
import javax.crypto.spec.{GCMParameterSpec, SecretKeySpec}
import org.apache.pekko.util.ByteString
import org.apache.openwhisk.core.scheduler.queue._
import org.junit.runner.RunWith
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.junit.JUnitRunner
import spray.json.DefaultJsonProtocol._
import spray.json._

@RunWith(classOf[JUnitRunner])
class ProtectedEnvelopeV1Tests extends AnyFlatSpec with Matchers {
  private val resourceName = "protected-envelope-v1-vectors.json"

  behavior of "ProtectedEnvelopeV1"

  it should "reproduce the Gateway P1 canonical vectors byte for byte" in {
    val bytes = readResource(resourceName)
    toHex(MessageDigest.getInstance("SHA-256").digest(bytes)) shouldBe
      "159a66b267c71cff9d681977ff19c95f294549088f3c8c310c3d0c11310c9319"

    val vectors = new String(bytes, StandardCharsets.UTF_8).parseJson.asJsObject
      .fields("protected_envelope_v1_vectors")
      .convertTo[JsArray]
      .elements
    vectors should have size 4

    vectors.foreach { value =>
      val fields = value.asJsObject.fields
      val key = fromHex(string(fields, "key"))
      val plaintext = fromHex(string(fields, "plaintext"))
      val expectedHeader = fromHex(string(fields, "header"))
      val expectedCiphertext = fromHex(string(fields, "ciphertext"))
      val expectedTag = fromHex(string(fields, "tag"))
      val expectedEnvelope = fromHex(string(fields, "envelope"))

      val decoded = ProtectedEnvelopeV1.decode(expectedEnvelope).fold(message => fail(message), envelope => envelope)
      decoded.header shouldBe expectedHeader
      decoded.ciphertext shouldBe expectedCiphertext
      decoded.tag shouldBe expectedTag
      decoded.bytes shouldBe expectedEnvelope
      decoded.bindingId.toString shouldBe string(fields, "binding_id")
      (decoded.kind.id & 0xff) shouldBe number(fields, "kind")
      (decoded.direction.id & 0xff) shouldBe number(fields, "direction")
      (decoded.correlationKind.id & 0xff) shouldBe number(fields, "correlation_kind")
      decoded.correlationHash shouldBe fromHex(string(fields, "correlation_hash"))
      decoded.iv shouldBe fromHex(string(fields, "iv"))

      val cipher = Cipher.getInstance("AES/GCM/NoPadding")
      cipher.init(
        Cipher.ENCRYPT_MODE,
        new SecretKeySpec(key.toArray, "AES"),
        new GCMParameterSpec(128, decoded.iv.toArray))
      cipher.updateAAD(decoded.header.toArray)
      val encrypted = ByteString.fromArray(cipher.doFinal(plaintext.toArray))
      encrypted.dropRight(ProtectedEnvelopeV1.TagSize) shouldBe expectedCiphertext
      encrypted.takeRight(ProtectedEnvelopeV1.TagSize) shouldBe expectedTag
    }
  }

  it should "reject an IV whose domain does not match the encoded direction" in {
    val bytes = fromHex(
      "010202020102030405060708202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f" +
        "8123456789abcdeffedcba98000000119b2b68362c2bfc7c1b7b677dc258009575" +
        "a1e04dd3de226e0fab54d7161854d568")

    ProtectedEnvelopeV1.decode(bytes).left.get should include("IV domain")
  }

  private def readResource(name: String): Array[Byte] = {
    val input = Option(getClass.getClassLoader.getResourceAsStream(name)).getOrElse(fail(s"missing $name"))
    try input.readAllBytes()
    finally input.close()
  }

  private def fromHex(value: String): ByteString =
    ByteString.fromArray(value.grouped(2).map(Integer.parseInt(_, 16).toByte).toArray)

  private def toHex(value: Array[Byte]): String = value.map(byte => f"${byte & 0xff}%02x").mkString

  private def string(fields: Map[String, JsValue], name: String): String = fields(name).convertTo[String]

  private def number(fields: Map[String, JsValue], name: String): Int = fields(name).convertTo[Int]
}
