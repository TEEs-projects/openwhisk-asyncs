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

import java.nio.{ByteBuffer, ByteOrder}

import org.apache.pekko.util.ByteString

sealed abstract class ProtectedObjectKind(val id: Byte)
object ProtectedObjectKind {
  case object Code extends ProtectedObjectKind(1)
  case object Input extends ProtectedObjectKind(2)
  case object Result extends ProtectedObjectKind(3)

  def fromId(id: Byte): Either[String, ProtectedObjectKind] = id match {
    case Code.id   => Right(Code)
    case Input.id  => Right(Input)
    case Result.id => Right(Result)
    case other     => Left(s"unsupported protected object kind ${other & 0xff}")
  }
}

sealed abstract class ProtectedEnvelopeDirection(val id: Byte, val ivDomain: Int)
object ProtectedEnvelopeDirection {
  case object SourceToGateway extends ProtectedEnvelopeDirection(1, 0)
  case object GatewayToTarget extends ProtectedEnvelopeDirection(2, 0)
  case object TargetToGateway extends ProtectedEnvelopeDirection(3, 1)
  case object GatewayToSource extends ProtectedEnvelopeDirection(4, 1)

  def fromId(id: Byte): Either[String, ProtectedEnvelopeDirection] = id match {
    case SourceToGateway.id => Right(SourceToGateway)
    case GatewayToTarget.id => Right(GatewayToTarget)
    case TargetToGateway.id => Right(TargetToGateway)
    case GatewayToSource.id => Right(GatewayToSource)
    case other              => Left(s"unsupported protected envelope direction ${other & 0xff}")
  }
}

sealed abstract class ProtectedCorrelationKind(val id: Byte)
object ProtectedCorrelationKind {
  case object CodeObject extends ProtectedCorrelationKind(1)
  case object RequestId extends ProtectedCorrelationKind(2)

  def fromId(id: Byte): Either[String, ProtectedCorrelationKind] = id match {
    case CodeObject.id => Right(CodeObject)
    case RequestId.id  => Right(RequestId)
    case other         => Left(s"unsupported protected correlation kind ${other & 0xff}")
  }
}

/** Canonical byte-level codec for Gateway P1 ProtectedEnvelopeV1. */
final case class ProtectedEnvelopeV1(kind: ProtectedObjectKind,
                                     direction: ProtectedEnvelopeDirection,
                                     bindingId: Long,
                                     correlationKind: ProtectedCorrelationKind,
                                     correlationHash: ByteString,
                                     iv: ByteString,
                                     ciphertext: ByteString,
                                     tag: ByteString) {
  require(bindingId > 0, "protected envelope binding id must be positive")
  require(correlationHash.length == ProtectedEnvelopeV1.CorrelationHashSize, "correlation hash must be 32 bytes")
  require(iv.length == ProtectedEnvelopeV1.IvSize, "AES-GCM IV must be 12 bytes")
  require(tag.length == ProtectedEnvelopeV1.TagSize, "AES-GCM tag must be 16 bytes")
  require(ciphertext.length <= Int.MaxValue, "protected envelope ciphertext is too large")
  require(ProtectedEnvelopeV1.ivDomain(iv) == direction.ivDomain, "AES-GCM IV direction domain does not match")

  lazy val header: ByteString = {
    val output = ByteBuffer.allocate(ProtectedEnvelopeV1.HeaderSize).order(ByteOrder.BIG_ENDIAN)
    output.put(ProtectedEnvelopeV1.Version)
    output.put(kind.id)
    output.put(direction.id)
    output.put(correlationKind.id)
    output.putLong(bindingId)
    output.put(correlationHash.toArray)
    output.put(iv.toArray)
    output.putInt(ciphertext.length)
    ByteString.fromArray(output.array())
  }

  lazy val bytes: ByteString = header ++ ciphertext ++ tag
}

object ProtectedEnvelopeV1 {
  val Version: Byte = 1
  val HeaderSize = 60
  val CorrelationHashSize = 32
  val IvSize = 12
  val TagSize = 16
  val MinimumSize: Int = HeaderSize + TagSize

  def decode(bytes: ByteString): Either[String, ProtectedEnvelopeV1] = {
    if (bytes.length < MinimumSize) {
      Left(s"protected envelope is too short: ${bytes.length} bytes")
    } else {
      val input = ByteBuffer.wrap(bytes.toArray).order(ByteOrder.BIG_ENDIAN)
      val version = input.get()
      val kindId = input.get()
      val directionId = input.get()
      val correlationKindId = input.get()
      val bindingId = input.getLong()
      val correlationHash = new Array[Byte](CorrelationHashSize)
      input.get(correlationHash)
      val iv = new Array[Byte](IvSize)
      input.get(iv)
      val payloadLength = input.getInt()

      for {
        _ <- Either.cond(version == Version, (), s"unsupported protected envelope version ${version & 0xff}")
        kind <- ProtectedObjectKind.fromId(kindId)
        direction <- ProtectedEnvelopeDirection.fromId(directionId)
        correlationKind <- ProtectedCorrelationKind.fromId(correlationKindId)
        _ <- Either.cond(bindingId > 0, (), "protected envelope binding id must be positive")
        _ <- Either.cond(payloadLength >= 0, (), "protected envelope payload length is negative")
        _ <- Either.cond(
          bytes.length == HeaderSize + payloadLength + TagSize,
          (),
          s"protected envelope length ${bytes.length} does not match payload length $payloadLength")
        ivBytes = ByteString.fromArray(iv)
        _ <- Either.cond(
          ivDomain(ivBytes) == direction.ivDomain,
          (),
          s"protected envelope IV domain does not match direction ${direction.id & 0xff}")
        ciphertext = bytes.slice(HeaderSize, HeaderSize + payloadLength)
        tag = bytes.takeRight(TagSize)
      } yield
        ProtectedEnvelopeV1(
          kind,
          direction,
          bindingId,
          correlationKind,
          ByteString.fromArray(correlationHash),
          ivBytes,
          ciphertext,
          tag)
    }
  }

  private[queue] def ivDomain(iv: ByteString): Int = (iv.head & 0x80) >>> 7
}
