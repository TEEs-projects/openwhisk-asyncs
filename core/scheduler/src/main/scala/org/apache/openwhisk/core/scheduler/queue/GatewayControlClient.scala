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

package org.apache.openwhisk.core.scheduler.queue

import java.io.{EOFException, InputStream}
import java.net.{InetSocketAddress, Socket}
import java.nio.charset.StandardCharsets
import java.nio.{ByteBuffer, ByteOrder}

import org.apache.pekko.util.ByteString

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{blocking, ExecutionContext, Future}
import scala.util.control.NonFatal

final case class GatewayControlConfig(enabled: Boolean,
                                      host: String,
                                      port: Int,
                                      connectTimeout: FiniteDuration,
                                      readTimeout: FiniteDuration,
                                      maxEnvelopeBytes: Int)

sealed abstract class GatewayControlError(val message: String)
final case class GatewayControlProtocolError(override val message: String) extends GatewayControlError(message)
final case class GatewayControlTransportError(override val message: String) extends GatewayControlError(message)
final case class GatewayControlRejected(status: Long)
    extends GatewayControlError(s"Gateway rejected control operation: status=$status")

final case class GatewayRegisteredTarget(targetBindingId: Long, peerSessionId: Long)

trait GatewayControlClient {
  def registerTarget(containerIdentity: String,
                     endpointHost: String,
                     endpointPort: Int): Future[Either[GatewayControlError, GatewayRegisteredTarget]] =
    Future.successful(Left(GatewayControlProtocolError("target registration is not implemented")))

  def closeTarget(targetBindingId: Long): Future[Either[GatewayControlError, Unit]] =
    Future.successful(Left(GatewayControlProtocolError("target close is not implemented")))

  def reencryptToTarget(targetBindingId: Long,
                        sourceEnvelope: ProtectedEnvelopeV1): Future[Either[GatewayControlError, ProtectedEnvelopeV1]]

  def reencryptResultToSource(
    sourceBindingId: Long,
    targetBindingId: Long,
    targetEnvelope: ProtectedEnvelopeV1): Future[Either[GatewayControlError, ProtectedEnvelopeV1]] =
    Future.successful(Left(GatewayControlProtocolError("result finalization is not implemented")))
}

/** Client for Gateway P1's Linux x86_64 FIFO-over-TCP control wire. */
final class P1GatewayControlClient(config: GatewayControlConfig)(implicit ec: ExecutionContext)
    extends GatewayControlClient {
  import P1GatewayControlClient._

  require(config.host.nonEmpty, "Gateway control host must not be empty")
  require(config.port > 0 && config.port <= 65535, "Gateway control port is invalid")
  require(config.connectTimeout.toMillis > 0, "Gateway connect timeout must be positive")
  require(config.readTimeout.toMillis > 0, "Gateway read timeout must be positive")
  require(config.maxEnvelopeBytes >= ProtectedEnvelopeV1.MinimumSize, "Gateway max envelope size is too small")

  override def registerTarget(containerIdentity: String,
                              endpointHost: String,
                              endpointPort: Int): Future[Either[GatewayControlError, GatewayRegisteredTarget]] =
    attempt {
      for {
        identity <- encodeCString(containerIdentity, TargetContainerIdentitySize, "container identity")
        host <- encodeCString(endpointHost, TargetEndpointHostSize, "target endpoint host")
        _ <- Either.cond(
          endpointPort > 0 && endpointPort <= 65535,
          (),
          GatewayControlProtocolError("target endpoint port is invalid"))
      } yield {
        val body = ByteBuffer.allocate(RegisterTargetRequestSize).order(ByteOrder.BIG_ENDIAN)
        body.put(ControlVersion)
        body.put(Array.fill[Byte](3)(0))
        body.putShort(endpointPort.toShort)
        body.putShort(0.toShort)
        body.put(identity)
        body.put(host)
        decodeRegisterTarget(
          exchange(
            RegisterTargetRequestType,
            RegisterTargetResponseType,
            ByteString.fromArray(body.array()),
            RegisterTargetResponseSize))
      }
    }

  override def closeTarget(targetBindingId: Long): Future[Either[GatewayControlError, Unit]] =
    attempt {
      Either
        .cond(targetBindingId > 0, (), GatewayControlProtocolError("target binding id must be positive"))
        .map { _ =>
          val body = ByteBuffer.allocate(CloseTargetRequestSize).order(ByteOrder.BIG_ENDIAN)
          body.put(ControlVersion)
          body.put(Array.fill[Byte](7)(0))
          body.putLong(targetBindingId)
          decodeStatus(
            exchange(
              CloseTargetRequestType,
              CloseTargetResponseType,
              ByteString.fromArray(body.array()),
              StatusResponseSize),
            "close target")
        }
    }

  override def reencryptToTarget(
    targetBindingId: Long,
    sourceEnvelope: ProtectedEnvelopeV1): Future[Either[GatewayControlError, ProtectedEnvelopeV1]] =
    attempt {
      validateToTarget(targetBindingId, sourceEnvelope).map { _ =>
        reencrypt(
          ReencryptToTargetOperation,
          sourceEnvelope.bindingId,
          targetBindingId,
          sourceEnvelope,
          ProtectedEnvelopeDirection.GatewayToTarget,
          targetBindingId)
      }
    }

  override def reencryptResultToSource(
    sourceBindingId: Long,
    targetBindingId: Long,
    targetEnvelope: ProtectedEnvelopeV1): Future[Either[GatewayControlError, ProtectedEnvelopeV1]] =
    attempt {
      validateToSource(sourceBindingId, targetBindingId, targetEnvelope).map { _ =>
        reencrypt(
          ReencryptResultToSourceOperation,
          sourceBindingId,
          targetBindingId,
          targetEnvelope,
          ProtectedEnvelopeDirection.GatewayToSource,
          sourceBindingId)
      }
    }

  private def attempt[T](operation: => Either[GatewayControlError, T]): Future[Either[GatewayControlError, T]] =
    Future {
      blocking {
        try operation
        catch {
          case error: GatewayControlException => Left(error.error)
          case NonFatal(t) =>
            Left(GatewayControlTransportError(s"Gateway control exchange failed: ${t.getClass.getSimpleName}"))
        }
      }
    }

  private def validateToTarget(targetBindingId: Long,
                               envelope: ProtectedEnvelopeV1): Either[GatewayControlError, Unit] =
    for {
      _ <- Either.cond(targetBindingId > 0, (), GatewayControlProtocolError("target binding id must be positive"))
      _ <- Either.cond(
        envelope.direction == ProtectedEnvelopeDirection.SourceToGateway,
        (),
        GatewayControlProtocolError("Gateway input envelope must use S2G direction"))
      _ <- Either.cond(
        envelope.kind == ProtectedObjectKind.Code || envelope.kind == ProtectedObjectKind.Input,
        (),
        GatewayControlProtocolError("Gateway to-target operation accepts only CODE or INPUT"))
      _ <- validateEnvelopeSize(envelope)
    } yield ()

  private def validateToSource(sourceBindingId: Long,
                               targetBindingId: Long,
                               envelope: ProtectedEnvelopeV1): Either[GatewayControlError, Unit] =
    for {
      _ <- Either.cond(sourceBindingId > 0, (), GatewayControlProtocolError("source binding id must be positive"))
      _ <- Either.cond(targetBindingId > 0, (), GatewayControlProtocolError("target binding id must be positive"))
      _ <- Either.cond(
        envelope.direction == ProtectedEnvelopeDirection.TargetToGateway,
        (),
        GatewayControlProtocolError("Gateway result envelope must use T2G direction"))
      _ <- Either.cond(
        envelope.bindingId == targetBindingId,
        (),
        GatewayControlProtocolError("Gateway result envelope has the wrong target binding"))
      _ <- Either.cond(
        envelope.kind == ProtectedObjectKind.Result,
        (),
        GatewayControlProtocolError("Gateway result operation accepts only RESULT"))
      _ <- validateEnvelopeSize(envelope)
    } yield ()

  private def validateEnvelopeSize(envelope: ProtectedEnvelopeV1): Either[GatewayControlError, Unit] =
    Either.cond(
      envelope.bytes.length <= config.maxEnvelopeBytes,
      (),
      GatewayControlProtocolError("Gateway input envelope exceeds configured maximum"))

  private def reencrypt(operation: Byte,
                        sourceBindingId: Long,
                        targetBindingId: Long,
                        inputEnvelope: ProtectedEnvelopeV1,
                        expectedDirection: ProtectedEnvelopeDirection,
                        expectedBindingId: Long): ProtectedEnvelopeV1 = {
    val envelope = inputEnvelope.bytes
    val body = ByteBuffer.allocate(ReencryptRequestPrefixSize + envelope.length).order(ByteOrder.BIG_ENDIAN)
    body.put(ControlVersion)
    body.put(operation)
    body.putShort(0.toShort)
    body.putLong(sourceBindingId)
    body.putLong(targetBindingId)
    body.putInt(envelope.length)
    body.put(envelope.toArray)
    decodeReencryptResponse(
      operation,
      inputEnvelope,
      expectedDirection,
      expectedBindingId,
      exchange(
        ReencryptRequestType,
        ReencryptResponseType,
        ByteString.fromArray(body.array()),
        ReencryptResponsePrefixSize,
        ReencryptResponsePrefixSize.toLong + config.maxEnvelopeBytes))
  }

  private def exchange(requestType: Int,
                       responseType: Int,
                       requestBody: ByteString,
                       exactResponseSize: Int): ByteString =
    exchange(requestType, responseType, requestBody, exactResponseSize, exactResponseSize)

  private def exchange(requestType: Int,
                       responseType: Int,
                       requestBody: ByteString,
                       minResponseSize: Long,
                       maxResponseSize: Long): ByteString = {
    val socket = new Socket()
    try {
      socket.connect(new InetSocketAddress(config.host, config.port), timeoutMillis(config.connectTimeout))
      socket.setSoTimeout(timeoutMillis(config.readTimeout))
      val output = socket.getOutputStream
      output.write(encodeOuterFrame(requestType, requestBody.length).toArray)
      output.write(requestBody.toArray)
      output.flush()
      socket.shutdownOutput()

      val input = socket.getInputStream
      val responseHeader = decodeOuterHeader(readExactly(input, OuterHeaderSize))
      if (responseHeader.messageType != responseType) {
        fail(GatewayControlProtocolError(s"unexpected Gateway response type ${responseHeader.messageType}"))
      }
      if (responseHeader.bodySize < minResponseSize || responseHeader.bodySize > maxResponseSize) {
        fail(GatewayControlProtocolError(s"invalid Gateway response body size ${responseHeader.bodySize}"))
      }
      readExactly(input, responseHeader.bodySize.toInt)
    } finally {
      socket.close()
    }
  }

  private def decodeRegisterTarget(body: ByteString): GatewayRegisteredTarget = {
    val input = ByteBuffer.wrap(body.toArray).order(ByteOrder.BIG_ENDIAN)
    val version = input.get()
    val state = input.get()
    val reserved = input.getShort()
    val status = input.getInt() & 0xffffffffL
    val targetBindingId = input.getLong()
    val peerSessionId = input.getInt() & 0xffffffffL
    if (version != ControlVersion || reserved != 0 || state != TargetActiveState) {
      fail(GatewayControlProtocolError("Gateway target registration did not return ACTIVE"))
    }
    if (status != 0) fail(GatewayControlRejected(status))
    if (targetBindingId <= 0) {
      fail(GatewayControlProtocolError("Gateway target registration returned an invalid target binding id"))
    }
    GatewayRegisteredTarget(targetBindingId, peerSessionId)
  }

  private def decodeStatus(body: ByteString, operation: String): Unit = {
    val input = ByteBuffer.wrap(body.toArray).order(ByteOrder.BIG_ENDIAN)
    val version = input.get()
    val reserved = Array.fill[Byte](3)(0)
    input.get(reserved)
    val status = input.getInt() & 0xffffffffL
    if (version != ControlVersion || reserved.exists(_ != 0)) {
      fail(GatewayControlProtocolError(s"invalid Gateway $operation response"))
    }
    if (status != 0) fail(GatewayControlRejected(status))
  }

  private def decodeReencryptResponse(operation: Byte,
                                      inputEnvelope: ProtectedEnvelopeV1,
                                      expectedDirection: ProtectedEnvelopeDirection,
                                      expectedBindingId: Long,
                                      body: ByteString): ProtectedEnvelopeV1 = {
    val input = ByteBuffer.wrap(body.toArray).order(ByteOrder.BIG_ENDIAN)
    val version = input.get()
    val responseOperation = input.get()
    val reserved = input.getShort()
    val status = input.getInt() & 0xffffffffL
    val envelopeLength = input.getInt()

    if (version != ControlVersion || responseOperation != operation || reserved != 0) {
      fail(GatewayControlProtocolError("invalid Gateway re-encryption response prefix"))
    }
    if (status != 0) fail(GatewayControlRejected(status))
    if (envelopeLength < 0 || body.length != ReencryptResponsePrefixSize + envelopeLength) {
      fail(GatewayControlProtocolError("Gateway response envelope length mismatch"))
    }

    val outputEnvelope = ProtectedEnvelopeV1
      .decode(body.drop(ReencryptResponsePrefixSize))
      .fold(message => fail(GatewayControlProtocolError(s"invalid Gateway output envelope: $message")), identity)
    if (outputEnvelope.direction != expectedDirection ||
        outputEnvelope.bindingId != expectedBindingId ||
        outputEnvelope.kind != inputEnvelope.kind ||
        outputEnvelope.correlationKind != inputEnvelope.correlationKind ||
        outputEnvelope.correlationHash != inputEnvelope.correlationHash) {
      fail(GatewayControlProtocolError("Gateway output envelope does not preserve bound correlation"))
    }
    outputEnvelope
  }

  private def encodeCString(value: String, size: Int, description: String): Either[GatewayControlError, Array[Byte]] = {
    val bytes = value.getBytes(StandardCharsets.UTF_8)
    Either.cond(
      value.nonEmpty && bytes.length < size && !bytes.contains(0.toByte),
      bytes ++ Array.fill[Byte](size - bytes.length)(0),
      GatewayControlProtocolError(s"$description is empty or too long"))
  }

  private def timeoutMillis(duration: FiniteDuration): Int = math.min(duration.toMillis, Int.MaxValue.toLong).toInt
}

object P1GatewayControlClient {
  private val ControlVersion: Byte = 1
  private val ReencryptToTargetOperation: Byte = 1
  private val ReencryptResultToSourceOperation: Byte = 2
  private val TargetActiveState: Byte = 2

  // FIFO_MSG_TYPE values from Gateway P1 Include/fifo_def.h (zero-based enum).
  private[queue] val RegisterTargetRequestType = 8
  private[queue] val RegisterTargetResponseType = 9
  private[queue] val CloseTargetRequestType = 10
  private[queue] val CloseTargetResponseType = 11
  private[queue] val ReencryptRequestType = 12
  private[queue] val ReencryptResponseType = 13

  private[queue] val OuterHeaderSize = 16
  private val TargetContainerIdentitySize = 128
  private val TargetEndpointHostSize = 64
  private val RegisterTargetRequestSize = 200
  private val RegisterTargetResponseSize = 20
  private val CloseTargetRequestSize = 16
  private val StatusResponseSize = 8
  private val ReencryptRequestPrefixSize = 24
  private[queue] val ReencryptResponsePrefixSize = 12

  private final case class OuterHeader(messageType: Int, bodySize: Long)
  private final case class GatewayControlException(error: GatewayControlError) extends RuntimeException(error.message)

  private[queue] def encodeOuterFrame(messageType: Int, bodySize: Int): ByteString = {
    val output = ByteBuffer.allocate(OuterHeaderSize).order(ByteOrder.nativeOrder())
    output.putInt(messageType)
    output.putLong(bodySize.toLong)
    output.putInt(0)
    ByteString.fromArray(output.array())
  }

  private def decodeOuterHeader(bytes: ByteString): OuterHeader = {
    val input = ByteBuffer.wrap(bytes.toArray).order(ByteOrder.nativeOrder())
    val messageType = input.getInt()
    val bodySize = input.getLong()
    input.getInt()
    if (bodySize < 0) fail(GatewayControlProtocolError("negative Gateway FIFO body size"))
    OuterHeader(messageType, bodySize)
  }

  private[queue] def readExactly(input: InputStream, size: Int): ByteString = {
    val bytes = new Array[Byte](size)
    var offset = 0
    while (offset < size) {
      val count = input.read(bytes, offset, size - offset)
      if (count < 0) throw new EOFException(s"Gateway control response ended after $offset of $size bytes")
      offset += count
    }
    ByteString.fromArray(bytes)
  }

  private def fail(error: GatewayControlError): Nothing = throw GatewayControlException(error)
}
