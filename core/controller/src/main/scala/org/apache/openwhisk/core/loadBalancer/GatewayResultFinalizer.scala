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

package org.apache.openwhisk.core.loadBalancer

import java.util.Base64

import org.apache.openwhisk.core.entity.{ActivationResponse, WhiskActivation}
import org.apache.openwhisk.common.Logging
import org.apache.openwhisk.core.scheduler.queue.{GatewayControlClient, TargetBoundActivationContent}
import spray.json._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

trait GatewayResultFinalizer {
  def finalizeResult(activation: WhiskActivation): Future[Either[String, WhiskActivation]]
}

object GatewayResultFinalizer {
  val ClientEnvelopeField = "__reusable_protected_result_envelope"
  val RequestIdHashField = "__reusable_request_id_hash"
  val SourceBindingField = "__reusable_source_binding_id"

  object Unconfigured extends GatewayResultFinalizer {
    override def finalizeResult(activation: WhiskActivation): Future[Either[String, WhiskActivation]] =
      Future.successful {
        if (containsTargetResult(activation.response.result)) {
          Left("Gateway result finalizer is unavailable")
        } else Right(activation)
      }
  }

  def containsTargetResult(result: Option[JsValue]): Boolean = result.exists {
    case JsObject(fields) => fields.contains(TargetBoundActivationContent.ResultRootField)
    case _                => false
  }

  def systemError(activation: WhiskActivation, reason: String): WhiskActivation =
    activation.copy(response = ActivationResponse.whiskError(s"target result finalization failed: $reason"))
}

final class P1GatewayResultFinalizer(gatewayClient: GatewayControlClient)(implicit ec: ExecutionContext,
                                                                          logging: Logging)
    extends GatewayResultFinalizer {

  private val timingNode = sys.env.get("C1_TIMING_NODE_ID").orElse(sys.env.get("HOSTNAME")).getOrElse("")
  private val timingPid = java.lang.management.ManagementFactory.getRuntimeMXBean.getName.takeWhile(_ != '@')

  private def timingValue(value: String): String =
    Option(value).getOrElse("").replace('|', '_').replace('\n', ' ').replace('\r', ' ')

  private def emitGatewayTiming(activation: WhiskActivation,
                                eventCode: String,
                                boundaryName: String,
                                sourceBindingId: Long,
                                targetBindingId: Long,
                                requestIdHash: String,
                                status: String): Unit = {
    val fields = Seq(
      "event_code" -> eventCode,
      "boundary_name" -> boundaryName,
      "activation_id" -> activation.activationId.asString,
      "operation" -> "RESULT",
      "source_binding_id" -> sourceBindingId.toString,
      "target_binding_id" -> targetBindingId.toString,
      "request_id_hash" -> requestIdHash,
      "status" -> status,
      "node" -> timingNode,
      "process" -> "openwhisk_controller",
      "pid" -> timingPid,
      "tid" -> "gateway-result-finalizer",
      "unix_ns" -> (System.currentTimeMillis() * 1000000L).toString,
      "mono_ns" -> System.nanoTime().toString,
      "clock_domain" -> "openwhisk_controller_jvm_mono")
    logging.info(
      this,
      s"C1TIMING_EVENT|${fields.map { case (key, value) => s"$key=${timingValue(value)}" }.mkString("|")}")
  }

  override def finalizeResult(activation: WhiskActivation): Future[Either[String, WhiskActivation]] = {
    if (!GatewayResultFinalizer.containsTargetResult(activation.response.result)) {
      Future.successful(Right(activation))
    } else {
      TargetBoundActivationContent.parseTargetResult(activation.response.result) match {
        case Left(error) => Future.successful(Left(error.targetReencryptionError))
        case Right(targetResult) =>
          emitGatewayTiming(
            activation,
            "RG810",
            "reusable_gateway_result_reencrypt_enter",
            targetResult.sourceBindingId,
            targetResult.targetBindingId,
            targetResult.requestIdHash,
            "started")
          gatewayClient
            .reencryptResultToSource(
              targetResult.sourceBindingId,
              targetResult.targetBindingId,
              targetResult.targetEnvelope)
            .map {
              case Left(error) =>
                emitGatewayTiming(
                  activation,
                  "RG820",
                  "reusable_gateway_result_reencrypt_exit",
                  targetResult.sourceBindingId,
                  targetResult.targetBindingId,
                  targetResult.requestIdHash,
                  "failure")
                Left(error.message)
              case Right(sourceEnvelope) =>
                emitGatewayTiming(
                  activation,
                  "RG820",
                  "reusable_gateway_result_reencrypt_exit",
                  targetResult.sourceBindingId,
                  targetResult.targetBindingId,
                  targetResult.requestIdHash,
                  "success")
                val result = JsObject(
                  GatewayResultFinalizer.ClientEnvelopeField -> JsString(
                    Base64.getEncoder.encodeToString(sourceEnvelope.bytes.toArray)),
                  GatewayResultFinalizer.RequestIdHashField -> JsString(targetResult.requestIdHash),
                  GatewayResultFinalizer.SourceBindingField -> JsString(targetResult.sourceBindingId.toString))
                Right(activation.copy(response = ActivationResponse.success(Some(result))))
            }
            .recover {
              case NonFatal(t) =>
                emitGatewayTiming(
                  activation,
                  "RG820",
                  "reusable_gateway_result_reencrypt_exit",
                  targetResult.sourceBindingId,
                  targetResult.targetBindingId,
                  targetResult.requestIdHash,
                  "exception")
                Left(s"Gateway result exchange failed: ${t.getClass.getSimpleName}")
            }
      }
    }
  }
}
