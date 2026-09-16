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

import java.net.{InetAddress, ServerSocket}
import java.nio.{ByteBuffer, ByteOrder}

import org.apache.pekko.util.ByteString
import org.apache.openwhisk.core.scheduler.queue._
import org.junit.runner.RunWith
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.junit.JUnitRunner

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

@RunWith(classOf[JUnitRunner])
class GatewayControlClientTests extends AnyFlatSpec with Matchers {
  private implicit val ec: ExecutionContext = ExecutionContext.global

  behavior of "P1GatewayControlClient"

  it should "exchange one canonical re-encryption frame over the P1 outer FIFO transport" in {
    val source = targetVector.copy(
      direction = ProtectedEnvelopeDirection.SourceToGateway,
      bindingId = 17,
      iv = fromHex("0123456789abcdeffedcba98"))
    withGateway { server =>
      val observed = Future {
        val socket = server.accept()
        try {
          val request = ByteString.fromArray(socket.getInputStream.readAllBytes())
          val outer = ByteBuffer
            .wrap(request.take(P1GatewayControlClient.OuterHeaderSize).toArray)
            .order(ByteOrder.nativeOrder())
          outer.getInt() shouldBe P1GatewayControlClient.ReencryptRequestType
          val bodySize = outer.getLong()
          outer.getInt() shouldBe 0
          bodySize shouldBe request.drop(P1GatewayControlClient.OuterHeaderSize).length.toLong

          val body = ByteBuffer
            .wrap(request.drop(P1GatewayControlClient.OuterHeaderSize).toArray)
            .order(ByteOrder.BIG_ENDIAN)
          body.get() shouldBe 1.toByte
          body.get() shouldBe 1.toByte
          body.getShort() shouldBe 0.toShort
          (body.getLong() == source.bindingId) shouldBe true
          body.getLong() shouldBe targetVector.bindingId
          val envelopeSize = body.getInt()
          val envelope = new Array[Byte](envelopeSize)
          body.get(envelope)
          ByteString.fromArray(envelope) shouldBe source.bytes

          val responseBody = encodeResponse(status = 0, targetVector.bytes)
          val output = socket.getOutputStream
          output.write(
            P1GatewayControlClient
              .encodeOuterFrame(P1GatewayControlClient.ReencryptResponseType, responseBody.length)
              .toArray)
          output.write(responseBody.toArray)
          output.flush()
        } finally socket.close()
      }

      val client = new P1GatewayControlClient(config(server.getLocalPort))
      Await.result(client.reencryptToTarget(targetVector.bindingId, source), 5.seconds) shouldBe Right(targetVector)
      Await.result(observed, 5.seconds)
    }
  }

  it should "return an explicit rejection without accepting an envelope" in {
    val source = targetVector.copy(
      direction = ProtectedEnvelopeDirection.SourceToGateway,
      bindingId = 17,
      iv = fromHex("0123456789abcdeffedcba98"))
    withGateway { server =>
      val response = Future {
        val socket = server.accept()
        try {
          socket.getInputStream.readAllBytes()
          val body = encodeResponse(status = 23, ByteString.empty)
          val output = socket.getOutputStream
          output.write(
            P1GatewayControlClient.encodeOuterFrame(P1GatewayControlClient.ReencryptResponseType, body.length).toArray)
          output.write(body.toArray)
          output.flush()
        } finally socket.close()
      }

      val client = new P1GatewayControlClient(config(server.getLocalPort))
      Await.result(client.reencryptToTarget(42, source), 5.seconds) shouldBe Left(GatewayControlRejected(23))
      Await.result(response, 5.seconds)
    }
  }

  it should "register an ACTIVE target and close the same binding over the P1 wire" in {
    withGateway { server =>
      val observed = Future {
        val register = server.accept()
        try {
          val request = ByteString.fromArray(register.getInputStream.readAllBytes())
          decodeOuterType(request) shouldBe P1GatewayControlClient.RegisterTargetRequestType
          val body = ByteBuffer
            .wrap(request.drop(P1GatewayControlClient.OuterHeaderSize).toArray)
            .order(ByteOrder.BIG_ENDIAN)
          body.get() shouldBe 1.toByte
          body.position(4)
          (body.getShort() & 0xffff) shouldBe 18888
          body.getShort() shouldBe 0.toShort
          val identity = new Array[Byte](128)
          val host = new Array[Byte](64)
          body.get(identity)
          body.get(host)
          new String(identity.takeWhile(_ != 0), "UTF-8") shouldBe "container-1"
          new String(host.takeWhile(_ != 0), "UTF-8") shouldBe "WORKER_HOST_0_PLACEHOLDER"

          val responseBody = ByteBuffer.allocate(20).order(ByteOrder.BIG_ENDIAN)
          responseBody.put(1.toByte).put(2.toByte).putShort(0).putInt(0).putLong(41).putInt(0)
          writeResponse(
            register,
            P1GatewayControlClient.RegisterTargetResponseType,
            ByteString.fromArray(responseBody.array()))
        } finally register.close()

        val close = server.accept()
        try {
          val request = ByteString.fromArray(close.getInputStream.readAllBytes())
          decodeOuterType(request) shouldBe P1GatewayControlClient.CloseTargetRequestType
          val body = ByteBuffer
            .wrap(request.drop(P1GatewayControlClient.OuterHeaderSize).toArray)
            .order(ByteOrder.BIG_ENDIAN)
          body.get() shouldBe 1.toByte
          body.position(8)
          body.getLong() shouldBe 41L
          val responseBody = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN)
          responseBody.put(1.toByte).put(Array.fill[Byte](3)(0)).putInt(0)
          writeResponse(close, P1GatewayControlClient.CloseTargetResponseType, ByteString.fromArray(responseBody.array()))
        } finally close.close()
      }

      val client = new P1GatewayControlClient(config(server.getLocalPort))
      Await.result(client.registerTarget("container-1", "WORKER_HOST_0_PLACEHOLDER", 18888), 5.seconds) shouldBe
        Right(GatewayRegisteredTarget(41, 0))
      Await.result(client.closeTarget(41), 5.seconds) shouldBe Right(())
      Await.result(observed, 5.seconds)
    }
  }

  it should "finalize a T2G result into a G2S envelope with source correlation" in {
    val targetResult = targetVector.copy(
      kind = ProtectedObjectKind.Result,
      direction = ProtectedEnvelopeDirection.TargetToGateway,
      iv = fromHex("8123456789abcdeffedcba98"))
    val sourceResult = targetResult.copy(
      direction = ProtectedEnvelopeDirection.GatewayToSource,
      bindingId = 17,
      iv = fromHex("9123456789abcdeffedcba98"))
    withGateway { server =>
      val observed = Future {
        val socket = server.accept()
        try {
          val request = ByteString.fromArray(socket.getInputStream.readAllBytes())
          decodeOuterType(request) shouldBe P1GatewayControlClient.ReencryptRequestType
          val body = ByteBuffer
            .wrap(request.drop(P1GatewayControlClient.OuterHeaderSize).toArray)
            .order(ByteOrder.BIG_ENDIAN)
          body.get() shouldBe 1.toByte
          body.get() shouldBe 2.toByte
          body.getShort() shouldBe 0.toShort
          body.getLong() shouldBe 17L
          body.getLong() shouldBe targetResult.bindingId
          val envelopeSize = body.getInt()
          val envelope = new Array[Byte](envelopeSize)
          body.get(envelope)
          ByteString.fromArray(envelope) shouldBe targetResult.bytes
          writeResponse(
            socket,
            P1GatewayControlClient.ReencryptResponseType,
            encodeResponse(status = 0, sourceResult.bytes, operation = 2))
        } finally socket.close()
      }

      val client = new P1GatewayControlClient(config(server.getLocalPort))
      Await.result(client.reencryptResultToSource(17, targetResult.bindingId, targetResult), 5.seconds) shouldBe
        Right(sourceResult)
      Await.result(observed, 5.seconds)
    }
  }

  private def withGateway(test: ServerSocket => Unit): Unit = {
    val server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress)
    try test(server)
    finally server.close()
  }

  private def config(port: Int) =
    GatewayControlConfig(
      enabled = true,
      host = InetAddress.getLoopbackAddress.getHostAddress,
      port = port,
      connectTimeout = 2.seconds,
      readTimeout = 2.seconds,
      maxEnvelopeBytes = 4096)

  private def encodeResponse(status: Int, envelope: ByteString, operation: Int = 1): ByteString = {
    val output = ByteBuffer
      .allocate(P1GatewayControlClient.ReencryptResponsePrefixSize + envelope.length)
      .order(ByteOrder.BIG_ENDIAN)
    output.put(1.toByte)
    output.put(operation.toByte)
    output.putShort(0)
    output.putInt(status)
    output.putInt(envelope.length)
    output.put(envelope.toArray)
    ByteString.fromArray(output.array())
  }

  private def decodeOuterType(request: ByteString): Int =
    ByteBuffer.wrap(request.take(P1GatewayControlClient.OuterHeaderSize).toArray).order(ByteOrder.nativeOrder()).getInt()

  private def writeResponse(socket: java.net.Socket, responseType: Int, body: ByteString): Unit = {
    val output = socket.getOutputStream
    output.write(P1GatewayControlClient.encodeOuterFrame(responseType, body.length).toArray)
    output.write(body.toArray)
    output.flush()
  }

  private val targetVector = ProtectedEnvelopeV1
    .decode(
      fromHex(
        "010202020102030405060708202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f" +
          "0123456789abcdeffedcba98000000119b2b68362c2bfc7c1b7b677dc258009575" +
          "a1e04dd3de226e0fab54d7161854d568"))
    .fold(message => fail(message), envelope => envelope)

  private def fromHex(value: String): ByteString =
    ByteString.fromArray(value.grouped(2).map(Integer.parseInt(_, 16).toByte).toArray)
}
