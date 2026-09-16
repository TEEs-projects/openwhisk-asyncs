/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import java.util.Base64

import org.apache.openwhisk.core.connector.ActivationMessage
import org.apache.openwhisk.common.Logging
import org.apache.openwhisk.core.database.ArtifactStore
import org.apache.openwhisk.core.entity._
import org.apache.openwhisk.core.scheduler.grpc.GetActivation
import spray.json._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

/** Same-fetch seam. H2 supplies the actual protected-code/input implementation. */
trait TargetBoundActivationDispatcher {
  def prepare(request: GetActivation,
              activation: ActivationMessage): Future[Either[TargetReencryptionError, ActivationMessage]]
}

object TargetBoundActivationDispatcher {

  /**
   * Preserve all existing profiles, but fail closed when a target-bound pull
   * reaches a scheduler that does not yet have the H2 dispatcher installed.
   */
  object Unconfigured extends TargetBoundActivationDispatcher {
    override def prepare(request: GetActivation,
                         activation: ActivationMessage): Future[Either[TargetReencryptionError, ActivationMessage]] =
      Future.successful(request.targetBindingId match {
        case None     => Right(activation)
        case Some(id) => Left(TargetReencryptionError(s"target-bound dispatcher unavailable for binding $id"))
      })
  }
}

final case class SourceProtectedCode(exactRevision: DocRevision, envelope: ProtectedEnvelopeV1)

trait ExactRevisionProtectedCodeProvider {
  def load(activation: ActivationMessage): Future[Either[TargetReencryptionError, SourceProtectedCode]]
}

object WhiskActionExactRevisionProtectedCodeProvider {
  type ActionLookup = (DocId, DocRevision, org.apache.openwhisk.common.TransactionId) => Future[WhiskAction]

  def apply(entityStore: ArtifactStore[WhiskEntity])(
    implicit ec: ExecutionContext): ExactRevisionProtectedCodeProvider = {
    val lookup: ActionLookup = (docId, revision, tid) => {
      implicit val transid = tid
      WhiskAction.get(entityStore, docId, revision, fromCache = false, ignoreMissingAttachment = false)
    }
    new WhiskActionExactRevisionProtectedCodeProvider(lookup)
  }
}

final class WhiskActionExactRevisionProtectedCodeProvider(
  lookup: WhiskActionExactRevisionProtectedCodeProvider.ActionLookup)(implicit ec: ExecutionContext)
    extends ExactRevisionProtectedCodeProvider {

  override def load(activation: ActivationMessage): Future[Either[TargetReencryptionError, SourceProtectedCode]] = {
    if (activation.revision == DocRevision.empty) {
      Future.successful(Left(TargetReencryptionError("target-bound code lookup requires an exact action revision")))
    } else {
      lookup(activation.action.toDocId, activation.revision, activation.transid)
        .map(extract(activation, _))
        .recover {
          case NonFatal(t) =>
            Left(TargetReencryptionError(s"exact-revision protected code lookup failed: ${t.getClass.getSimpleName}"))
        }
    }
  }

  private def extract(activation: ActivationMessage,
                      action: WhiskAction): Either[TargetReencryptionError, SourceProtectedCode] = {
    for {
      _ <- Either.cond(
        action.rev == activation.revision,
        (),
        TargetReencryptionError("exact-revision protected code lookup returned a different revision"))
      _ <- Either.cond(
        action.fullyQualifiedName(withVersion = false).toDocId == activation.action.toDocId,
        (),
        TargetReencryptionError("exact-revision protected code lookup returned a different action"))
      encoded <- action.exec match {
        case exec: CodeExec[_] if exec.binary =>
          exec.codeAsJson match {
            case JsString(value) => Right(value)
            case _               => Left(TargetReencryptionError("source-protected action code is not inline after lookup"))
          }
        case _: CodeExec[_] => Left(TargetReencryptionError("source-protected action code must be binary"))
        case _              => Left(TargetReencryptionError("target-bound action does not contain executable code"))
      }
      bytes <- TargetBoundActivationContent.decodeBase64(encoded, "source-protected action code")
      envelope <- ProtectedEnvelopeV1
        .decode(bytes)
        .left
        .map(message => TargetReencryptionError(s"invalid source-protected action code: $message"))
      _ <- TargetBoundActivationContent.requireEnvelope(
        envelope,
        ProtectedObjectKind.Code,
        ProtectedCorrelationKind.CodeObject,
        "source-protected action code")
    } yield SourceProtectedCode(activation.revision, envelope)
  }
}

final class GatewayTargetBoundActivationDispatcher(
  codeProvider: ExactRevisionProtectedCodeProvider,
  gatewayClient: GatewayControlClient)(implicit ec: ExecutionContext, logging: Logging)
    extends TargetBoundActivationDispatcher {

  private val timingNode = sys.env.get("C1_TIMING_NODE_ID").orElse(sys.env.get("HOSTNAME")).getOrElse("")
  private val timingPid = java.lang.management.ManagementFactory.getRuntimeMXBean.getName.takeWhile(_ != '@')

  private def timingValue(value: String): String =
    Option(value).getOrElse("").replace('|', '_').replace('\n', ' ').replace('\r', ' ')

  private def emitGatewayTiming(activation: ActivationMessage,
                                eventCode: String,
                                boundaryName: String,
                                targetBindingId: Long,
                                sourceInput: ProtectedEnvelopeV1,
                                warmed: Boolean,
                                status: String): Unit = {
    val fields = Seq(
      "event_code" -> eventCode,
      "boundary_name" -> boundaryName,
      "activation_id" -> activation.activationId.asString,
      "transaction_id" -> activation.transid.id,
      "operation" -> (if (warmed) "INPUT" else "CODE_INPUT"),
      "source_binding_id" -> sourceInput.bindingId.toString,
      "target_binding_id" -> targetBindingId.toString,
      "request_id_hash" -> sourceInput.correlationHash.map(byte => f"${byte & 0xff}%02x").mkString,
      "warmed" -> warmed.toString,
      "status" -> status,
      "node" -> timingNode,
      "process" -> "openwhisk_scheduler",
      "pid" -> timingPid,
      "tid" -> activation.transid.id,
      "unix_ns" -> (System.currentTimeMillis() * 1000000L).toString,
      "mono_ns" -> System.nanoTime().toString,
      "clock_domain" -> "openwhisk_scheduler_jvm_mono")
    logging.info(
      this,
      s"C1TIMING_EVENT|${fields.map { case (key, value) => s"$key=${timingValue(value)}" }.mkString("|")}")(
      activation.transid)
  }

  private def timedTargetReencryption(
    activation: ActivationMessage,
    targetBindingId: Long,
    sourceInput: ProtectedEnvelopeV1,
    warmed: Boolean)(operation: => Future[Either[TargetReencryptionError, ActivationMessage]])
    : Future[Either[TargetReencryptionError, ActivationMessage]] = {
    emitGatewayTiming(
      activation,
      "RG270",
      "reusable_gateway_target_reencrypt_enter",
      targetBindingId,
      sourceInput,
      warmed,
      "started")
    operation.andThen {
      case scala.util.Success(Right(_)) =>
        emitGatewayTiming(
          activation,
          "RG280",
          "reusable_gateway_target_reencrypt_exit",
          targetBindingId,
          sourceInput,
          warmed,
          "success")
      case scala.util.Success(Left(_)) =>
        emitGatewayTiming(
          activation,
          "RG280",
          "reusable_gateway_target_reencrypt_exit",
          targetBindingId,
          sourceInput,
          warmed,
          "failure")
      case scala.util.Failure(_) =>
        emitGatewayTiming(
          activation,
          "RG280",
          "reusable_gateway_target_reencrypt_exit",
          targetBindingId,
          sourceInput,
          warmed,
          "exception")
    }
  }

  override def prepare(request: GetActivation,
                       activation: ActivationMessage): Future[Either[TargetReencryptionError, ActivationMessage]] =
    request.targetBindingId match {
      case None => Future.successful(Right(activation))
      case Some(targetBindingId) if targetBindingId <= 0 =>
        Future.successful(Left(TargetReencryptionError("target binding id must be positive")))
      case Some(targetBindingId) => prepareTargetBound(request, activation, targetBindingId)
    }

  private def prepareTargetBound(request: GetActivation,
                                 activation: ActivationMessage,
                                 targetBindingId: Long): Future[Either[TargetReencryptionError, ActivationMessage]] = {
    if (activation.revision == DocRevision.empty) {
      Future.successful(Left(TargetReencryptionError("target-bound dispatch requires an exact action revision")))
    } else
      TargetBoundActivationContent.sourceInput(activation.content) match {
        case Left(error) => Future.successful(Left(error))
        case Right(sourceInput) if request.warmed =>
          timedTargetReencryption(activation, targetBindingId, sourceInput, warmed = true) {
            reencrypt(targetBindingId, sourceInput).map(
              _.map(
                targetInput =>
                  withTargetDispatch(
                    activation,
                    targetBindingId,
                    sourceInput.bindingId,
                    activation.revision,
                    warmed = true,
                    targetInput,
                    None)))
          }
        case Right(sourceInput) =>
          codeProvider.load(activation).flatMap {
            case Left(error) => Future.successful(Left(error))
            case Right(sourceCode) =>
              timedTargetReencryption(activation, targetBindingId, sourceInput, warmed = false) {
                reencrypt(targetBindingId, sourceCode.envelope).flatMap {
                  case Left(error) => Future.successful(Left(error))
                  case Right(targetCode) =>
                    reencrypt(targetBindingId, sourceInput).map(
                      _.map(
                        targetInput =>
                          withTargetDispatch(
                            activation,
                            targetBindingId,
                            sourceInput.bindingId,
                            sourceCode.exactRevision,
                            warmed = false,
                            targetInput,
                            Some(targetCode))))
                }
              }
          }
      }
  }

  private def withTargetDispatch(activation: ActivationMessage,
                                 targetBindingId: Long,
                                 sourceBindingId: Long,
                                 exactRevision: DocRevision,
                                 warmed: Boolean,
                                 targetInput: ProtectedEnvelopeV1,
                                 targetCode: Option[ProtectedEnvelopeV1]): ActivationMessage =
    activation.copy(
      content = Some(
        TargetBoundActivationContent
          .targetDispatch(targetBindingId, sourceBindingId, exactRevision, warmed, targetInput, targetCode)))

  private def reencrypt(
    targetBindingId: Long,
    sourceEnvelope: ProtectedEnvelopeV1): Future[Either[TargetReencryptionError, ProtectedEnvelopeV1]] =
    gatewayClient
      .reencryptToTarget(targetBindingId, sourceEnvelope)
      .map(_.left.map(error => TargetReencryptionError(error.message)))
      .recover {
        case NonFatal(t) =>
          Left(TargetReencryptionError(s"Gateway re-encryption failed: ${t.getClass.getSimpleName}"))
      }
}

/** JSON contract carried by ActivationMessage before and after same-fetch dispatch. */
object TargetBoundActivationContent {
  val RootField = "__ow_reusable_target_bound_v1"
  val SourceInputField = "sourceProtectedInputEnvelope"
  val TargetInputField = "targetProtectedInputEnvelope"
  val TargetCodeField = "targetProtectedCodeEnvelope"
  val TargetBindingField = "targetBindingId"
  val SourceBindingField = "sourceBindingId"
  val ExactRevisionField = "exactActionRevision"
  val WarmedField = "warmed"
  val WorkerCodeFetchField = "workerCodeFetch"
  val ResultRootField = "__ow_reusable_target_result_v1"
  val TargetResultField = "targetProtectedResultEnvelope"
  val RequestIdHashField = "requestIdHash"

  final case class TargetDispatch(targetBindingId: Long,
                                  sourceBindingId: Long,
                                  exactRevision: DocRevision,
                                  warmed: Boolean,
                                  targetInput: ProtectedEnvelopeV1,
                                  targetCode: Option[ProtectedEnvelopeV1])

  final case class TargetResult(sourceBindingId: Long,
                                targetBindingId: Long,
                                requestIdHash: String,
                                targetEnvelope: ProtectedEnvelopeV1)

  def sourceInput(content: Option[JsValue]): Either[TargetReencryptionError, ProtectedEnvelopeV1] =
    for {
      root <- content match {
        case Some(JsObject(fields)) if fields.keySet == Set(RootField) =>
          fields(RootField) match {
            case value: JsObject => Right(value)
            case _               => Left(TargetReencryptionError("target-bound source content root must be an object"))
          }
        case _ =>
          Left(TargetReencryptionError("target-bound activation must contain only the protected input contract"))
      }
      encoded <- root.fields.get(SourceInputField) match {
        case Some(JsString(value)) if root.fields.keySet == Set(SourceInputField) => Right(value)
        case _                                                                    => Left(TargetReencryptionError("target-bound activation is missing its source-protected INPUT"))
      }
      bytes <- decodeBase64(encoded, "source-protected INPUT")
      envelope <- ProtectedEnvelopeV1
        .decode(bytes)
        .left
        .map(message => TargetReencryptionError(s"invalid source-protected INPUT: $message"))
      _ <- requireEnvelope(
        envelope,
        ProtectedObjectKind.Input,
        ProtectedCorrelationKind.RequestId,
        "source-protected INPUT")
    } yield envelope

  def targetDispatch(targetBindingId: Long,
                     sourceBindingId: Long,
                     exactRevision: DocRevision,
                     warmed: Boolean,
                     targetInput: ProtectedEnvelopeV1,
                     targetCode: Option[ProtectedEnvelopeV1]): JsObject = {
    val fields = Map[String, JsValue](
      TargetBindingField -> JsString(targetBindingId.toString),
      SourceBindingField -> JsString(sourceBindingId.toString),
      ExactRevisionField -> JsString(exactRevision.rev),
      WarmedField -> JsBoolean(warmed),
      WorkerCodeFetchField -> JsBoolean(false),
      TargetInputField -> JsString(encodeBase64(targetInput.bytes))) ++ targetCode.map { code =>
      TargetCodeField -> JsString(encodeBase64(code.bytes))
    }
    JsObject(RootField -> JsObject(fields))
  }

  def parseTargetDispatch(content: Option[JsValue]): Either[TargetReencryptionError, TargetDispatch] =
    for {
      root <- singleRoot(content, RootField, "target-bound dispatch")
      targetBindingId <- positiveLong(root, TargetBindingField)
      sourceBindingId <- positiveLong(root, SourceBindingField)
      exactRevision <- root.fields.get(ExactRevisionField) match {
        case Some(JsString(value)) if value.nonEmpty => Right(DocRevision(value))
        case _                                       => Left(TargetReencryptionError("target-bound dispatch is missing its exact action revision"))
      }
      warmed <- root.fields.get(WarmedField) match {
        case Some(JsBoolean(value)) => Right(value)
        case _                      => Left(TargetReencryptionError("target-bound dispatch is missing warmed state"))
      }
      _ <- root.fields.get(WorkerCodeFetchField) match {
        case Some(JsBoolean(false)) => Right(())
        case _                      => Left(TargetReencryptionError("target-bound dispatch must disable worker code fetch"))
      }
      input <- decodeEnvelope(root, TargetInputField, "target-protected INPUT")
      _ <- requireTargetEnvelope(input, ProtectedObjectKind.Input, targetBindingId, "target-protected INPUT")
      code <- root.fields.get(TargetCodeField) match {
        case Some(_) => decodeEnvelope(root, TargetCodeField, "target-protected CODE").map(Some(_))
        case None    => Right(None)
      }
      _ <- code match {
        case Some(envelope) =>
          requireTargetEnvelope(envelope, ProtectedObjectKind.Code, targetBindingId, "target-protected CODE")
        case None => Right(())
      }
      expectedFields = Set(
        TargetBindingField,
        SourceBindingField,
        ExactRevisionField,
        WarmedField,
        WorkerCodeFetchField,
        TargetInputField) ++ code.map(_ => TargetCodeField)
      _ <- Either.cond(
        root.fields.keySet == expectedFields,
        (),
        TargetReencryptionError("target-bound dispatch contains unexpected fields"))
      _ <- Either.cond(
        warmed == code.isEmpty,
        (),
        TargetReencryptionError("cold target-bound dispatch requires CODE and warm dispatch must omit it"))
    } yield TargetDispatch(targetBindingId, sourceBindingId, exactRevision, warmed, input, code)

  def targetResult(result: TargetResult): JsObject =
    JsObject(
      ResultRootField -> JsObject(
        SourceBindingField -> JsString(result.sourceBindingId.toString),
        TargetBindingField -> JsString(result.targetBindingId.toString),
        RequestIdHashField -> JsString(result.requestIdHash),
        TargetResultField -> JsString(encodeBase64(result.targetEnvelope.bytes))))

  def parseTargetResult(result: Option[JsValue]): Either[TargetReencryptionError, TargetResult] =
    for {
      root <- singleRoot(result, ResultRootField, "target-bound result")
      _ <- Either.cond(
        root.fields.keySet == Set(SourceBindingField, TargetBindingField, RequestIdHashField, TargetResultField),
        (),
        TargetReencryptionError("target-bound result contains unexpected fields"))
      sourceBindingId <- positiveLong(root, SourceBindingField)
      targetBindingId <- positiveLong(root, TargetBindingField)
      requestIdHash <- root.fields.get(RequestIdHashField) match {
        case Some(JsString(value)) if value.matches("[0-9a-f]{64}") => Right(value)
        case _                                                      => Left(TargetReencryptionError("target-bound result has an invalid request id hash"))
      }
      envelope <- decodeEnvelope(root, TargetResultField, "target-protected RESULT")
      _ <- requireTargetEnvelope(envelope, ProtectedObjectKind.Result, targetBindingId, "target-protected RESULT")
      _ <- Either.cond(
        envelope.correlationHash.map(byte => f"${byte & 0xff}%02x").mkString == requestIdHash,
        (),
        TargetReencryptionError("target-bound result request correlation does not match its envelope"))
    } yield TargetResult(sourceBindingId, targetBindingId, requestIdHash, envelope)

  private def singleRoot(content: Option[JsValue],
                         field: String,
                         description: String): Either[TargetReencryptionError, JsObject] =
    content match {
      case Some(JsObject(fields)) if fields.keySet == Set(field) =>
        fields(field) match {
          case value: JsObject => Right(value)
          case _               => Left(TargetReencryptionError(s"$description root must be an object"))
        }
      case _ => Left(TargetReencryptionError(s"$description must contain only its protected root"))
    }

  private def positiveLong(root: JsObject, field: String): Either[TargetReencryptionError, Long] =
    root.fields.get(field) match {
      case Some(JsString(value)) =>
        try {
          val parsed = value.toLong
          Either.cond(parsed > 0, parsed, TargetReencryptionError(s"$field must be positive"))
        } catch {
          case _: NumberFormatException => Left(TargetReencryptionError(s"$field must be a decimal integer"))
        }
      case _ => Left(TargetReencryptionError(s"$field is missing"))
    }

  private def decodeEnvelope(root: JsObject,
                             field: String,
                             description: String): Either[TargetReencryptionError, ProtectedEnvelopeV1] =
    for {
      encoded <- root.fields.get(field) match {
        case Some(JsString(value)) => Right(value)
        case _                     => Left(TargetReencryptionError(s"$description is missing"))
      }
      bytes <- decodeBase64(encoded, description)
      envelope <- ProtectedEnvelopeV1
        .decode(bytes)
        .left
        .map(message => TargetReencryptionError(s"invalid $description: $message"))
    } yield envelope

  private def requireTargetEnvelope(envelope: ProtectedEnvelopeV1,
                                    kind: ProtectedObjectKind,
                                    targetBindingId: Long,
                                    description: String): Either[TargetReencryptionError, Unit] = {
    val expectedDirection =
      if (kind == ProtectedObjectKind.Result) ProtectedEnvelopeDirection.TargetToGateway
      else ProtectedEnvelopeDirection.GatewayToTarget
    for {
      _ <- Either.cond(
        envelope.direction == expectedDirection,
        (),
        TargetReencryptionError(s"$description has the wrong direction"))
      _ <- Either.cond(envelope.kind == kind, (), TargetReencryptionError(s"$description has the wrong object kind"))
      _ <- Either.cond(
        envelope.bindingId == targetBindingId,
        (),
        TargetReencryptionError(s"$description has the wrong target binding"))
      _ <- Either.cond(
        envelope.correlationKind ==
          (if (kind == ProtectedObjectKind.Code) ProtectedCorrelationKind.CodeObject
           else ProtectedCorrelationKind.RequestId),
        (),
        TargetReencryptionError(s"$description has the wrong correlation kind"))
    } yield ()
  }

  private[queue] def requireEnvelope(envelope: ProtectedEnvelopeV1,
                                     kind: ProtectedObjectKind,
                                     correlationKind: ProtectedCorrelationKind,
                                     description: String): Either[TargetReencryptionError, Unit] =
    for {
      _ <- Either.cond(
        envelope.direction == ProtectedEnvelopeDirection.SourceToGateway,
        (),
        TargetReencryptionError(s"$description must use S2G direction"))
      _ <- Either.cond(envelope.kind == kind, (), TargetReencryptionError(s"$description has the wrong object kind"))
      _ <- Either.cond(
        envelope.correlationKind == correlationKind,
        (),
        TargetReencryptionError(s"$description has the wrong correlation kind"))
    } yield ()

  private[queue] def decodeBase64(
    value: String,
    description: String): Either[TargetReencryptionError, org.apache.pekko.util.ByteString] =
    try {
      Right(org.apache.pekko.util.ByteString.fromArray(Base64.getDecoder.decode(value)))
    } catch {
      case _: IllegalArgumentException => Left(TargetReencryptionError(s"$description is not valid base64"))
    }

  private[queue] def encodeBase64(bytes: org.apache.pekko.util.ByteString): String =
    Base64.getEncoder.encodeToString(bytes.toArray)
}
