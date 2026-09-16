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

package org.apache.openwhisk.core.controller

import java.lang.management.ManagementFactory
import java.util.Base64
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.security.MessageDigest
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}
import org.apache.kafka.common.errors.RecordTooLargeException
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.model.StatusCodes._
import org.apache.pekko.http.scaladsl.server.RequestContext
import org.apache.pekko.http.scaladsl.server.RouteResult
import org.apache.pekko.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import org.apache.pekko.http.scaladsl.unmarshalling._
import spray.json._
import spray.json.DefaultJsonProtocol._
import org.apache.openwhisk.common.TransactionId
import org.apache.openwhisk.core.{FeatureFlags, WhiskConfig}
import org.apache.openwhisk.core.controller.RestApiCommons.{ListLimit, ListSkip}
import org.apache.openwhisk.core.controller.actions.{
  BackendPressureActivationResult,
  BackendPressureMetadata,
  PostActionActivation
}
import org.apache.openwhisk.core.database.{ActivationStore, CacheChangeNotification, NoDocumentException}
import org.apache.openwhisk.core.entitlement._
import org.apache.openwhisk.core.entity._
import org.apache.openwhisk.core.entity.types.EntityStore
import org.apache.openwhisk.http.ErrorResponse.terminate
import org.apache.openwhisk.http.Messages
import org.apache.openwhisk.http.Messages._
import org.apache.openwhisk.core.entitlement.Resource
import org.apache.openwhisk.core.entitlement.Collection
import org.apache.openwhisk.core.loadBalancer.LoadBalancerException
import org.apache.openwhisk.core.scheduler.queue.{ProtectedCorrelationKind, ProtectedEnvelopeDirection, ProtectedEnvelopeV1, ProtectedObjectKind, TargetBoundActivationContent}
import pureconfig._
import org.apache.openwhisk.core.ConfigKeys

/**
 * A singleton object which defines the properties that must be present in a configuration
 * in order to implement the actions API.
 */
object WhiskActionsApi {
  def requiredProperties = Map(WhiskConfig.actionSequenceMaxLimit -> null)

  /**
   * Amends annotations on an action create/update with system defined values.
   * This method currently adds the following annotations:
   * 1. [[Annotations.ProvideApiKeyAnnotationName]] with the value false iff the annotation is not already defined in the action annotations
   * 2. An [[execAnnotation]] consistent with the action kind; this annotation is always added and overrides a pre-existing value
   */
  protected[core] def amendAnnotations(annotations: Parameters, exec: Exec, create: Boolean = true): Parameters = {
    val newAnnotations = if (create && FeatureFlags.requireApiKeyAnnotation) {
      // these annotations are only added on newly created actions
      // since they can break existing actions created before the
      // annotation was created
      annotations
        .get(Annotations.ProvideApiKeyAnnotationName)
        .map(_ => annotations)
        .getOrElse {
          annotations ++ Parameters(Annotations.ProvideApiKeyAnnotationName, JsFalse)
        }
    } else annotations
    newAnnotations ++ execAnnotation(exec)
  }

  /**
   * Constructs an "exec" annotation. This is redundant with the exec kind
   * information available in WhiskAction but necessary for some clients which
   * fetch action lists but cannot determine action kinds without fetching them.
   * An alternative is to include the exec in the action list "view" but this
   * will require an API change. So using an annotation instead.
   */
  private def execAnnotation(exec: Exec): Parameters = {
    Parameters(WhiskAction.execFieldName, exec.kind)
  }
}

private[controller] final case class C1BackendPressurePreparedRequest(ordinal: Int,
                                                                      logicalRequestId: String,
                                                                      expectedRid: String,
                                                                      actionParams: JsObject)

private[controller] final case class C1BackendPressureActionIdentity(canonicalFqen: JsObject, revision: String)

private[controller] object C1BackendPressurePreparedRequests {
  private val asyncsProfile = "asyncs"
  private val reusableConcurrencyProfile = "reusable-concurrency"
  private val schemaVersions = Map(
    asyncsProfile -> "c1-asyncs-premeasurement-requests-v1",
    reusableConcurrencyProfile -> "c1-reusable-concurrency-premeasurement-requests-v1")

  def actionIdentity(action: WhiskActionMetaData): C1BackendPressureActionIdentity =
    C1BackendPressureActionIdentity(
      JsObject(
        "path" -> JsString(action.namespace.asString),
        "name" -> JsString(action.name.asString),
        "version" -> JsString(action.version.toString),
        "binding" -> action.binding.map(path => JsString(path.asString)).getOrElse(JsNull)),
      action.rev.asString)

  def submitEvidenceFields(runId: String,
                           request: C1BackendPressurePreparedRequest,
                           node: String,
                           processId: String,
                           transactionId: String,
                           unixNs: Long,
                           monoNs: Long): Seq[(String, String)] =
    Seq(
      "event_code" -> "BP010",
      "boundary_name" -> "backend_pressure_request_generated",
      "run_id" -> runId,
      "logical_request_id" -> request.logicalRequestId,
      "attempt_id" -> "1",
      "expected_rid" -> request.expectedRid,
      "node" -> node,
      "process" -> "backend_pressure_controller",
      "pid" -> processId,
      "tid" -> transactionId,
      "unix_ns" -> unixNs.toString,
      "mono_ns" -> monoNs.toString,
      "clock_domain" -> "openwhisk_controller_jvm_mono",
      "status" -> "observed")

  private def requiredString(fields: Map[String, JsValue], name: String): String =
    fields.get(name).map(_.convertTo[String]).getOrElse(deserializationError(s"$name is required"))

  private def requiredInt(fields: Map[String, JsValue], name: String): Int =
    fields.get(name).map(_.convertTo[Int]).getOrElse(deserializationError(s"$name is required"))

  private def requiredObject(fields: Map[String, JsValue], name: String): JsObject =
    fields.get(name).map(_.asJsObject).getOrElse(deserializationError(s"$name is required"))

  private def sha256(path: java.nio.file.Path): String = {
    val digest = MessageDigest.getInstance("SHA-256")
    val input = Files.newInputStream(path)
    val buffer = new Array[Byte](8192)
    try {
      var read = input.read(buffer)
      while (read >= 0) {
        if (read > 0) {
          digest.update(buffer, 0, read)
        }
        read = input.read(buffer)
      }
    } finally {
      input.close()
    }
    digest.digest().map(byte => f"${byte & 0xff}%02x").mkString
  }

  private def validateActionParams(profile: String,
                                   ordinal: Int,
                                   logicalRequestId: String,
                                   expectedRid: String,
                                   actionParams: JsObject): Unit = {
    if (profile == asyncsProfile) {
      val actionFields = actionParams.fields
      if (requiredString(actionFields, "logical_request_id") != logicalRequestId) {
        deserializationError(s"prepared request row $ordinal action logical_request_id does not match")
      }
      if (requiredString(actionFields, "attempt_id") != "1") {
        deserializationError(s"prepared request row $ordinal attempt_id must be 1")
      }
    } else if (profile == reusableConcurrencyProfile) {
      val outer = actionParams.fields
      if (outer.keySet != Set(TargetBoundActivationContent.RootField)) {
        deserializationError(s"prepared request row $ordinal must contain only the reusable protected-input root")
      }
      val root = outer(TargetBoundActivationContent.RootField).asJsObject.fields
      if (root.keySet != Set(TargetBoundActivationContent.SourceInputField)) {
        deserializationError(s"prepared request row $ordinal must contain only sourceProtectedInputEnvelope")
      }
      val encoded = requiredString(root, TargetBoundActivationContent.SourceInputField)
      val envelope = try {
        ProtectedEnvelopeV1
          .decode(org.apache.pekko.util.ByteString.fromArray(Base64.getDecoder.decode(encoded)))
          .fold(message => deserializationError(s"prepared request row $ordinal has an invalid INPUT envelope: $message"), identity)
      } catch {
        case NonFatal(_) => deserializationError(s"prepared request row $ordinal has invalid base64 INPUT")
      }
      val correlationHash = envelope.correlationHash.map(byte => f"${byte & 0xff}%02x").mkString
      if (envelope.kind != ProtectedObjectKind.Input ||
          envelope.direction != ProtectedEnvelopeDirection.SourceToGateway ||
          envelope.correlationKind != ProtectedCorrelationKind.RequestId ||
          correlationHash != expectedRid) {
        deserializationError(s"prepared request row $ordinal does not match the reusable INPUT/RID contract")
      }
    } else {
      deserializationError(s"prepared request profile is unsupported: $profile")
    }
  }

  private def loadUnsafe(sourceDirectory: String,
                         expectedCount: Int,
                         expectedWorkloadId: String,
                         profile: String): Vector[C1BackendPressurePreparedRequest] = {
    val source = Paths.get(sourceDirectory)
    val manifestPath = source.resolve("manifest.json")
    if (!Files.isRegularFile(manifestPath)) {
      deserializationError(s"prepared request manifest is not ready: $manifestPath")
    }
    val manifest = new String(Files.readAllBytes(manifestPath), StandardCharsets.UTF_8).parseJson.asJsObject.fields
    val schemaVersion = schemaVersions.getOrElse(profile, deserializationError(s"prepared request profile is unsupported: $profile"))
    if (requiredString(manifest, "schema_version") != schemaVersion) {
      deserializationError(s"prepared request schema_version must be $schemaVersion")
    }
    if (requiredInt(manifest, "configured_request_count") != expectedCount) {
      deserializationError(s"prepared request count must equal planned_logical_requests=$expectedCount")
    }
    if (manifest.get("failure_probability").map(_.convertTo[Double]).getOrElse(-1.0) != 0.0) {
      deserializationError("prepared request failure_probability must be 0")
    }
    val workload = requiredObject(manifest, "workload").fields
    if (requiredString(workload, "workload_id") != expectedWorkloadId) {
      deserializationError(s"prepared request workload_id must be $expectedWorkloadId")
    }

    val requestsPath = source.resolve(requiredString(manifest, "requests_file"))
    if (!Files.isRegularFile(requestsPath)) {
      deserializationError(s"prepared request data is missing: $requestsPath")
    }
    val expectedSha256 = requiredString(manifest, "requests_sha256")
    if (sha256(requestsPath) != expectedSha256) {
      deserializationError("prepared request data sha256 does not match manifest")
    }

    val rows = Vector.newBuilder[C1BackendPressurePreparedRequest]
    val reader = Files.newBufferedReader(requestsPath, StandardCharsets.UTF_8)
    try {
      var ordinal = 1
      var line = reader.readLine()
      while (line != null) {
        val fields = line.parseJson.asJsObject.fields
        val rowOrdinal = requiredInt(fields, "ordinal")
        val logicalRequestId = requiredString(fields, "logical_request_id")
        val expectedLogicalRequestId = ordinal.toString
        if (rowOrdinal != ordinal || logicalRequestId != expectedLogicalRequestId) {
          deserializationError(s"prepared request row $ordinal is out of order")
        }
        val actionParams = requiredObject(fields, "action_params")
        val expectedRid = requiredString(fields, "expected_rid")
        validateActionParams(profile, ordinal, logicalRequestId, expectedRid, actionParams)
        rows += C1BackendPressurePreparedRequest(
          ordinal,
          logicalRequestId,
          expectedRid,
          actionParams)
        ordinal += 1
        line = reader.readLine()
      }
    } finally {
      reader.close()
    }
    val result = rows.result()
    if (result.size != expectedCount) {
      deserializationError(s"prepared request rows=${result.size} must equal planned_logical_requests=$expectedCount")
    }
    result
  }

  def load(sourceDirectory: String,
           expectedCount: Int,
           expectedWorkloadId: String,
           profile: String = asyncsProfile): Either[String, Vector[C1BackendPressurePreparedRequest]] =
    try {
      Right(loadUnsafe(sourceDirectory, expectedCount, expectedWorkloadId, profile))
    } catch {
      case NonFatal(error) => Left(Option(error.getMessage).getOrElse(error.getClass.getSimpleName))
    }
}

/** A trait implementing the actions API. */
trait WhiskActionsApi extends WhiskCollectionAPI with PostActionActivation with ReferencedEntities {
  services: WhiskServices =>

  protected override val collection = Collection(Collection.ACTIONS)

  /** An actor system for timed based futures. */
  protected implicit val actorSystem: ActorSystem

  /** Database service to CRUD actions. */
  protected val entityStore: EntityStore

  /** Notification service for cache invalidation. */
  protected implicit val cacheChangeNotification: Some[CacheChangeNotification]

  /** Database service to get activations. */
  protected val activationStore: ActivationStore

  /** Config flag for Execute Only for Actions in Shared Packages */
  protected def executeOnly =
    loadConfigOrThrow[Boolean](ConfigKeys.sharedPackageExecuteOnly)

  /** Entity normalizer to JSON object. */
  import RestApiCommons.emptyEntityToJsObject

  /** JSON response formatter. */
  import RestApiCommons.jsonDefaultResponsePrinter

  private case class C1BackendPressureRequest(namespace: String,
                                              action: String,
                                              profile: String,
                                              run_id: String,
                                              concurrency: Int,
                                              logical_requests: Int,
                                              failure_probability: Double,
                                              payload_bytes: Int,
                                              workload_id: String,
                                              request_generation_mode: String,
                                              target_arrival_rate_per_sec: Option[Double],
                                              duration_sec: Option[Int],
                                              planned_logical_requests: Int,
                                              ramp_rate_schedule_per_sec: Option[Vector[Double]] = None,
                                              ramp_stage_duration_sec: Option[Int] = None,
                                              stage_gate_policy_id: Option[String] = None,
                                              stage_gate_source_submit_failure_limit: Option[Int] = None,
                                              stage_gate_source_schedule_lag_p95_ms: Option[Double] = None,
                                              stage_gate_source_schedule_lag_max_ms: Option[Double] = None,
                                              completion_window_size: Option[Int] = None,
                                              completion_window_timeout_sec: Option[Int] = None,
                                              plateau_policy_enabled: Option[Boolean] = None,
                                              plateau_window_size: Option[Int] = None,
                                              plateau_warmup_completions: Option[Int] = None,
                                              plateau_warmup_sec: Option[Int] = None,
                                              plateau_no_improve_sec: Option[Int] = None,
                                              plateau_min_improvement_fraction: Option[Double] = None,
                                              scheduler_fallback_retry_enabled: Option[Boolean] = None,
                                              scheduler_fallback_retry_limit: Option[Int] = None,
                                              prepared_request_source: Option[String] = None)

  private implicit object C1BackendPressureRequestFormat extends RootJsonFormat[C1BackendPressureRequest] {
    private def required[T](fields: Map[String, JsValue], name: String)(implicit reader: JsonReader[T]): T =
      fields.get(name).map(_.convertTo[T]).getOrElse(deserializationError(s"$name is required"))

    private def optional[T](fields: Map[String, JsValue], name: String)(implicit reader: JsonReader[T]): Option[T] =
      fields.get(name) match {
        case Some(JsNull) => None
        case Some(value)  => Some(value.convertTo[T])
        case None         => None
      }

    override def read(value: JsValue): C1BackendPressureRequest = {
      val fields = value.asJsObject.fields
      C1BackendPressureRequest(
        namespace = required[String](fields, "namespace"),
        action = required[String](fields, "action"),
        profile = required[String](fields, "profile"),
        run_id = required[String](fields, "run_id"),
        concurrency = required[Int](fields, "concurrency"),
        logical_requests = required[Int](fields, "logical_requests"),
        failure_probability = required[Double](fields, "failure_probability"),
        payload_bytes = required[Int](fields, "payload_bytes"),
        workload_id = required[String](fields, "workload_id"),
        request_generation_mode = required[String](fields, "request_generation_mode"),
        target_arrival_rate_per_sec = optional[Double](fields, "target_arrival_rate_per_sec"),
        duration_sec = optional[Int](fields, "duration_sec"),
        planned_logical_requests = required[Int](fields, "planned_logical_requests"),
        ramp_rate_schedule_per_sec = optional[Vector[Double]](fields, "ramp_rate_schedule_per_sec"),
        ramp_stage_duration_sec = optional[Int](fields, "ramp_stage_duration_sec"),
        stage_gate_policy_id = optional[String](fields, "stage_gate_policy_id"),
        stage_gate_source_submit_failure_limit = optional[Int](fields, "stage_gate_source_submit_failure_limit"),
        stage_gate_source_schedule_lag_p95_ms = optional[Double](fields, "stage_gate_source_schedule_lag_p95_ms"),
        stage_gate_source_schedule_lag_max_ms = optional[Double](fields, "stage_gate_source_schedule_lag_max_ms"),
        completion_window_size = optional[Int](fields, "completion_window_size"),
        completion_window_timeout_sec = optional[Int](fields, "completion_window_timeout_sec"),
        plateau_policy_enabled = optional[Boolean](fields, "plateau_policy_enabled"),
        plateau_window_size = optional[Int](fields, "plateau_window_size"),
        plateau_warmup_completions = optional[Int](fields, "plateau_warmup_completions"),
        plateau_warmup_sec = optional[Int](fields, "plateau_warmup_sec"),
        plateau_no_improve_sec = optional[Int](fields, "plateau_no_improve_sec"),
        plateau_min_improvement_fraction = optional[Double](fields, "plateau_min_improvement_fraction"),
        scheduler_fallback_retry_enabled = optional[Boolean](fields, "scheduler_fallback_retry_enabled"),
        scheduler_fallback_retry_limit = optional[Int](fields, "scheduler_fallback_retry_limit"),
        prepared_request_source = optional[String](fields, "prepared_request_source"))
    }

    override def write(request: C1BackendPressureRequest): JsValue = {
      val baseFields: Map[String, JsValue] = Map(
        "namespace" -> JsString(request.namespace),
        "action" -> JsString(request.action),
        "profile" -> JsString(request.profile),
        "run_id" -> JsString(request.run_id),
        "concurrency" -> JsNumber(request.concurrency),
        "logical_requests" -> JsNumber(request.logical_requests),
        "failure_probability" -> JsNumber(request.failure_probability),
        "payload_bytes" -> JsNumber(request.payload_bytes),
        "workload_id" -> JsString(request.workload_id),
        "request_generation_mode" -> JsString(request.request_generation_mode),
        "planned_logical_requests" -> JsNumber(request.planned_logical_requests))
      val optionalFields: Map[String, JsValue] = Seq(
        request.target_arrival_rate_per_sec.map("target_arrival_rate_per_sec" -> JsNumber(_)),
        request.duration_sec.map("duration_sec" -> JsNumber(_)),
        request.ramp_rate_schedule_per_sec.map(rates => "ramp_rate_schedule_per_sec" -> JsArray(rates.map(JsNumber(_)))),
        request.ramp_stage_duration_sec.map("ramp_stage_duration_sec" -> JsNumber(_)),
        request.stage_gate_policy_id.map("stage_gate_policy_id" -> JsString(_)),
        request.stage_gate_source_submit_failure_limit.map("stage_gate_source_submit_failure_limit" -> JsNumber(_)),
        request.stage_gate_source_schedule_lag_p95_ms.map("stage_gate_source_schedule_lag_p95_ms" -> JsNumber(_)),
        request.stage_gate_source_schedule_lag_max_ms.map("stage_gate_source_schedule_lag_max_ms" -> JsNumber(_)),
        request.completion_window_size.map("completion_window_size" -> JsNumber(_)),
        request.completion_window_timeout_sec.map("completion_window_timeout_sec" -> JsNumber(_)),
        request.plateau_policy_enabled.map("plateau_policy_enabled" -> JsBoolean(_)),
        request.plateau_window_size.map("plateau_window_size" -> JsNumber(_)),
        request.plateau_warmup_completions.map("plateau_warmup_completions" -> JsNumber(_)),
        request.plateau_warmup_sec.map("plateau_warmup_sec" -> JsNumber(_)),
        request.plateau_no_improve_sec.map("plateau_no_improve_sec" -> JsNumber(_)),
        request.plateau_min_improvement_fraction.map("plateau_min_improvement_fraction" -> JsNumber(_)),
        request.scheduler_fallback_retry_enabled.map("scheduler_fallback_retry_enabled" -> JsBoolean(_)),
        request.scheduler_fallback_retry_limit.map("scheduler_fallback_retry_limit" -> JsNumber(_)),
        request.prepared_request_source.map("prepared_request_source" -> JsString(_))).flatten.toMap
      JsObject(baseFields ++ optionalFields)
    }
  }

  private val c1BackendPressureMode = "controller-internal-queue"
  private val c1BackendPressureRequestModeOpenLoop = "open_loop_rate"
  private val c1BackendPressureRequestModeRamp = "open_loop_ramp"
  private val c1BackendPressureRequestModeStageGatedRamp = "open_loop_stage_gated_ramp"
  private val c1BackendPressureRequestModeCompletionWindow = "completion_window"
  private val c1BackendPressureStageGatePolicySourceLagV1 = "source_lag_v1"
  private val c1BackendPressureControllerSourceEnv = "C1_BACKEND_PRESSURE_CONTROLLER_SOURCE"
  private val c1BackendPressureMaxPayloadBytes = 1048576
  private val c1BackendPressureTimingProcessId = ManagementFactory.getRuntimeMXBean.getName.takeWhile(_ != '@')
  private val c1BackendPressureTimingNode =
    sys.env.get("C1_TIMING_NODE_ID").orElse(sys.env.get("HOSTNAME")).getOrElse("")

  private case class C1BackendPressureOutcome(logicalRequestId: String,
                                              result: Either[String, BackendPressureActivationResult],
                                              sourceScheduleLagNs: Long)

  private case class C1BackendPressureScheduleEntry(logicalRequestId: Int,
                                                    plannedSubmitOffsetNs: Long,
                                                    rampStageIndex: Option[Int] = None,
                                                    rampStageRatePerSec: Option[Double] = None,
                                                    rampStageStartOffsetNs: Option[Long] = None,
                                                    rampStageEndOffsetNs: Option[Long] = None)

  private def c1BackendPressureControllerSourceEnabled: Boolean =
    sys.env.get(c1BackendPressureControllerSourceEnv).contains("1")

  private def c1BackendPressureValue(value: String): String =
    Option(value).getOrElse("").replace('|', '_').replace('\n', ' ').replace('\r', ' ')

  private def validateC1BackendPressureRequest(request: C1BackendPressureRequest): Option[String] = {
    def errorIf(condition: Boolean, message: String): Option[String] = if (condition) Some(message) else None

    val modeErrors = request.request_generation_mode match {
      case `c1BackendPressureRequestModeOpenLoop` =>
        val planned = request.target_arrival_rate_per_sec
          .zip(request.duration_sec)
          .map { case (rate, duration) => math.floor(rate * duration).toInt }
        Seq(
          errorIf(request.target_arrival_rate_per_sec.forall(_ <= 0.0), "target_arrival_rate_per_sec must be positive"),
          errorIf(request.duration_sec.forall(_ <= 0), "duration_sec must be positive"),
          errorIf(
            planned.forall(_ != request.planned_logical_requests),
            "planned_logical_requests must equal floor(target_arrival_rate_per_sec * duration_sec)"),
          errorIf(
            request.logical_requests != request.planned_logical_requests,
            "logical_requests must equal planned_logical_requests for open_loop_rate"),
          errorIf(request.ramp_rate_schedule_per_sec.exists(_.nonEmpty), "ramp_rate_schedule_per_sec is only valid for open_loop_ramp"),
          errorIf(request.ramp_stage_duration_sec.nonEmpty, "ramp_stage_duration_sec is only valid for open_loop_ramp"),
          errorIf(request.stage_gate_policy_id.nonEmpty, "stage_gate_policy_id is only valid for open_loop_stage_gated_ramp"),
          errorIf(
            request.stage_gate_source_submit_failure_limit.nonEmpty,
            "stage_gate_source_submit_failure_limit is only valid for open_loop_stage_gated_ramp"),
          errorIf(
            request.stage_gate_source_schedule_lag_p95_ms.nonEmpty,
            "stage_gate_source_schedule_lag_p95_ms is only valid for open_loop_stage_gated_ramp"),
          errorIf(
            request.stage_gate_source_schedule_lag_max_ms.nonEmpty,
            "stage_gate_source_schedule_lag_max_ms is only valid for open_loop_stage_gated_ramp"),
          errorIf(request.completion_window_size.nonEmpty, "completion_window_size is only valid for completion_window"),
          errorIf(
            request.completion_window_timeout_sec.nonEmpty,
            "completion_window_timeout_sec is only valid for completion_window"),
          errorIf(request.plateau_policy_enabled.nonEmpty, "plateau_policy_enabled is only valid for completion_window"),
          errorIf(request.plateau_window_size.nonEmpty, "plateau_window_size is only valid for completion_window"),
          errorIf(request.plateau_warmup_completions.nonEmpty, "plateau_warmup_completions is only valid for completion_window"),
          errorIf(request.plateau_warmup_sec.nonEmpty, "plateau_warmup_sec is only valid for completion_window"),
          errorIf(request.plateau_no_improve_sec.nonEmpty, "plateau_no_improve_sec is only valid for completion_window"),
          errorIf(
            request.plateau_min_improvement_fraction.nonEmpty,
            "plateau_min_improvement_fraction is only valid for completion_window"),
          errorIf(
            request.scheduler_fallback_retry_enabled.nonEmpty,
            "scheduler_fallback_retry_enabled is only valid for completion_window"),
          errorIf(
            request.scheduler_fallback_retry_limit.nonEmpty,
            "scheduler_fallback_retry_limit is only valid for completion_window"))
      case `c1BackendPressureRequestModeRamp` | `c1BackendPressureRequestModeStageGatedRamp` =>
        val schedule = request.ramp_rate_schedule_per_sec.getOrElse(Vector.empty)
        val stageDuration = request.ramp_stage_duration_sec.getOrElse(0)
        val planned = schedule.map(rate => math.floor(rate * stageDuration).toInt).sum
        val isStageGated = request.request_generation_mode == c1BackendPressureRequestModeStageGatedRamp
        Seq(
          errorIf(schedule.isEmpty, s"ramp_rate_schedule_per_sec is required for ${request.request_generation_mode}"),
          errorIf(schedule.exists(_ <= 0.0), "ramp_rate_schedule_per_sec values must be positive"),
          errorIf(stageDuration <= 0, "ramp_stage_duration_sec must be positive"),
          errorIf(planned != request.planned_logical_requests, "planned_logical_requests must equal sum floor(ramp_rate * ramp_stage_duration_sec)"),
          errorIf(
            request.logical_requests != request.planned_logical_requests,
            s"logical_requests must equal planned_logical_requests for ${request.request_generation_mode}"),
          errorIf(
            isStageGated && request.stage_gate_policy_id.getOrElse("") != c1BackendPressureStageGatePolicySourceLagV1,
            s"stage_gate_policy_id must be $c1BackendPressureStageGatePolicySourceLagV1 for $c1BackendPressureRequestModeStageGatedRamp"),
          errorIf(
            isStageGated && request.stage_gate_source_submit_failure_limit.exists(_ < 0),
            "stage_gate_source_submit_failure_limit must be non-negative"),
          errorIf(
            isStageGated && request.stage_gate_source_schedule_lag_p95_ms.exists(_ < 0.0),
            "stage_gate_source_schedule_lag_p95_ms must be non-negative"),
          errorIf(
            isStageGated && request.stage_gate_source_schedule_lag_max_ms.exists(_ < 0.0),
            "stage_gate_source_schedule_lag_max_ms must be non-negative"),
          errorIf(
            !isStageGated && request.stage_gate_policy_id.nonEmpty,
            "stage_gate_policy_id is only valid for open_loop_stage_gated_ramp"),
          errorIf(
            !isStageGated && request.stage_gate_source_submit_failure_limit.nonEmpty,
            "stage_gate_source_submit_failure_limit is only valid for open_loop_stage_gated_ramp"),
          errorIf(
            !isStageGated && request.stage_gate_source_schedule_lag_p95_ms.nonEmpty,
            "stage_gate_source_schedule_lag_p95_ms is only valid for open_loop_stage_gated_ramp"),
          errorIf(
            !isStageGated && request.stage_gate_source_schedule_lag_max_ms.nonEmpty,
            "stage_gate_source_schedule_lag_max_ms is only valid for open_loop_stage_gated_ramp"),
          errorIf(request.completion_window_size.nonEmpty, "completion_window_size is only valid for completion_window"),
          errorIf(
            request.completion_window_timeout_sec.nonEmpty,
            "completion_window_timeout_sec is only valid for completion_window"),
          errorIf(request.plateau_policy_enabled.nonEmpty, "plateau_policy_enabled is only valid for completion_window"),
          errorIf(request.plateau_window_size.nonEmpty, "plateau_window_size is only valid for completion_window"),
          errorIf(request.plateau_warmup_completions.nonEmpty, "plateau_warmup_completions is only valid for completion_window"),
          errorIf(request.plateau_warmup_sec.nonEmpty, "plateau_warmup_sec is only valid for completion_window"),
          errorIf(request.plateau_no_improve_sec.nonEmpty, "plateau_no_improve_sec is only valid for completion_window"),
          errorIf(
            request.plateau_min_improvement_fraction.nonEmpty,
            "plateau_min_improvement_fraction is only valid for completion_window"),
          errorIf(
            request.scheduler_fallback_retry_enabled.nonEmpty,
            "scheduler_fallback_retry_enabled is only valid for completion_window"),
          errorIf(
            request.scheduler_fallback_retry_limit.nonEmpty,
            "scheduler_fallback_retry_limit is only valid for completion_window"))
      case `c1BackendPressureRequestModeCompletionWindow` =>
        Seq(
          errorIf(
            request.logical_requests != request.planned_logical_requests,
            s"logical_requests must equal planned_logical_requests for $c1BackendPressureRequestModeCompletionWindow"),
          errorIf(
            request.completion_window_size.forall(_ <= 0),
            "completion_window_size must be positive for completion_window"),
          errorIf(
            request.completion_window_timeout_sec.forall(_ <= 0),
            "completion_window_timeout_sec must be positive for completion_window"),
          errorIf(request.target_arrival_rate_per_sec.nonEmpty, "target_arrival_rate_per_sec is not valid for completion_window"),
          errorIf(request.duration_sec.nonEmpty, "duration_sec is not valid for completion_window"),
          errorIf(request.ramp_rate_schedule_per_sec.exists(_.nonEmpty), "ramp_rate_schedule_per_sec is not valid for completion_window"),
          errorIf(request.ramp_stage_duration_sec.nonEmpty, "ramp_stage_duration_sec is not valid for completion_window"),
          errorIf(request.stage_gate_policy_id.nonEmpty, "stage_gate_policy_id is only valid for open_loop_stage_gated_ramp"),
          errorIf(
            request.stage_gate_source_submit_failure_limit.nonEmpty,
            "stage_gate_source_submit_failure_limit is only valid for open_loop_stage_gated_ramp"),
          errorIf(
            request.stage_gate_source_schedule_lag_p95_ms.nonEmpty,
            "stage_gate_source_schedule_lag_p95_ms is only valid for open_loop_stage_gated_ramp"),
          errorIf(
            request.stage_gate_source_schedule_lag_max_ms.nonEmpty,
            "stage_gate_source_schedule_lag_max_ms is only valid for open_loop_stage_gated_ramp"),
          errorIf(request.plateau_window_size.exists(_ <= 0), "plateau_window_size must be positive"),
          errorIf(request.plateau_warmup_completions.exists(_ < 0), "plateau_warmup_completions must be non-negative"),
          errorIf(request.plateau_warmup_sec.exists(_ < 0), "plateau_warmup_sec must be non-negative"),
          errorIf(request.plateau_no_improve_sec.exists(_ <= 0), "plateau_no_improve_sec must be positive"),
          errorIf(
            request.plateau_min_improvement_fraction.exists(_ < 0.0),
            "plateau_min_improvement_fraction must be non-negative"),
          errorIf(
            request.scheduler_fallback_retry_limit.exists(_ < 0),
            "scheduler_fallback_retry_limit must be non-negative"))
      case _ =>
        Seq(Some(s"request_generation_mode must be $c1BackendPressureRequestModeOpenLoop, $c1BackendPressureRequestModeRamp, $c1BackendPressureRequestModeStageGatedRamp, or $c1BackendPressureRequestModeCompletionWindow"))
    }

    val errors = Seq(
      errorIf(request.namespace.trim.isEmpty, "namespace is required"),
      errorIf(request.action.trim.isEmpty, "action is required"),
      errorIf(request.profile.trim.isEmpty, "profile is required"),
      errorIf(request.run_id.trim.isEmpty, "run_id is required"),
      errorIf(request.concurrency <= 0, "concurrency must be positive"),
      errorIf(request.logical_requests <= 0, "logical_requests must be positive"),
      errorIf(request.planned_logical_requests <= 0, "planned_logical_requests must be positive"),
      errorIf(request.payload_bytes < 0, "payload_bytes must be non-negative"),
      errorIf(
        request.payload_bytes > c1BackendPressureMaxPayloadBytes,
        s"payload_bytes must be <= $c1BackendPressureMaxPayloadBytes"),
      errorIf(
        request.failure_probability < 0.0 || request.failure_probability > 1.0,
        "failure_probability must be between 0.0 and 1.0"),
      errorIf(request.workload_id.trim.isEmpty, "workload_id is required"),
      errorIf(
        request.prepared_request_source.nonEmpty &&
          !(Set("asyncs", "reusable-concurrency").contains(request.profile) &&
            request.request_generation_mode == c1BackendPressureRequestModeCompletionWindow),
        "prepared_request_source is only valid for asyncs or reusable-concurrency completion_window"),
      errorIf(
          Set("asyncs", "reusable-concurrency").contains(request.profile) &&
          request.request_generation_mode == c1BackendPressureRequestModeCompletionWindow &&
          request.prepared_request_source.forall(_.trim.isEmpty),
        "prepared_request_source is required for asyncs or reusable-concurrency completion_window"),
      errorIf(
          Set("asyncs", "reusable-concurrency").contains(request.profile) &&
          request.request_generation_mode == c1BackendPressureRequestModeCompletionWindow &&
          request.failure_probability != 0.0,
        "asyncs and reusable-concurrency completion_window currently require failure_probability=0")).flatten ++ modeErrors.flatten

    if (errors.isEmpty) None else Some(errors.mkString("; "))
  }

  private def c1BackendPressurePayload(request: C1BackendPressureRequest,
                                       scheduleEntry: C1BackendPressureScheduleEntry): JsObject = {
    val baseFields: Map[String, JsValue] = Map(
      "collection_id" -> JsString("C1"),
      "entry_mode" -> JsString(c1BackendPressureMode),
      "db_store_policy" -> JsString("skipped-for-backend-pressure"),
      "pressure_source" -> JsString("controller-internal"),
      "profile" -> JsString(request.profile),
      "run_id" -> JsString(request.run_id),
      "logical_request_id" -> JsString(scheduleEntry.logicalRequestId.toString),
      "concurrency" -> JsNumber(request.concurrency),
      "request_generation_mode" -> JsString(request.request_generation_mode),
      "target_arrival_rate_per_sec" -> JsNumber(scheduleEntry.rampStageRatePerSec.orElse(request.target_arrival_rate_per_sec).getOrElse(0.0)),
      "duration_sec" -> JsNumber(request.duration_sec.orElse(request.ramp_stage_duration_sec).getOrElse(0)),
      "planned_logical_requests" -> JsNumber(request.planned_logical_requests),
      "failure_probability" -> JsNumber(request.failure_probability),
      "payload_bytes" -> JsNumber(request.payload_bytes),
      "workload_id" -> JsString(request.workload_id),
      "payload" -> JsString("x" * request.payload_bytes))
    val rampFields: Map[String, JsValue] = scheduleEntry.rampStageIndex
      .map { stageIndex =>
        Map(
          "ramp_stage_index" -> JsNumber(stageIndex),
          "ramp_stage_rate_per_sec" -> JsNumber(scheduleEntry.rampStageRatePerSec.getOrElse(0.0)),
          "ramp_stage_start_offset_ns" -> JsNumber(scheduleEntry.rampStageStartOffsetNs.getOrElse(0L)),
          "ramp_stage_end_offset_ns" -> JsNumber(scheduleEntry.rampStageEndOffsetNs.getOrElse(0L)),
          "planned_submit_offset_ns" -> JsNumber(scheduleEntry.plannedSubmitOffsetNs))
      }
      .getOrElse(Map.empty[String, JsValue])
    JsObject(baseFields ++ rampFields)
  }

  private def c1BackendPressureResponse(runId: String,
                                        requestGenerationMode: String,
                                        submitted: Int,
                                        completed: Int,
                                        failed: Int,
                                        notReady: Int,
                                        results: Seq[C1BackendPressureOutcome]): JsObject = {
    val activationIds = results.collect {
      case C1BackendPressureOutcome(_, Right(result), _) => JsString(result.activationId.asString)
    }.toVector
    val errors = results.collect {
      case C1BackendPressureOutcome(_, Left(error), _) => JsString(error)
    }.toVector
    val statuses = results.map {
      case C1BackendPressureOutcome(logicalRequestId, Right(result), _) =>
        JsObject(
          "logical_request_id" -> JsString(logicalRequestId),
          "activation_id" -> JsString(result.activationId.asString),
          "attempt_id" -> JsNumber(result.attemptId),
          "status" -> JsString(result.status),
          "reason" -> JsString(result.reason))
      case C1BackendPressureOutcome(logicalRequestId, Left(error), _) =>
        JsObject(
          "logical_request_id" -> JsString(logicalRequestId),
          "status" -> JsString(BackendPressureActivationResult.Failed),
          "reason" -> JsString(error))
    }.toVector

    JsObject(
      "mode" -> JsString(c1BackendPressureMode),
      "run_id" -> JsString(runId),
      "request_generation_mode" -> JsString(requestGenerationMode),
      "submitted" -> JsNumber(submitted),
      "completed" -> JsNumber(completed),
      "failed" -> JsNumber(failed),
      "not_ready" -> JsNumber(notReady),
      "activation_ids" -> JsArray(activationIds),
      "errors" -> JsArray(errors),
      "statuses" -> JsArray(statuses))
  }

  private def c1BackendPressureAckResponse(request: C1BackendPressureRequest, submitted: Int): JsObject = {
    JsObject(
      "accepted" -> JsBoolean(true),
      "mode" -> JsString(c1BackendPressureMode),
      "run_id" -> JsString(request.run_id),
      "request_generation_mode" -> JsString(request.request_generation_mode),
      "response_model" -> JsString("ack_durable_logs"),
      "durable_data_source" -> JsString("controller_filtered_logs"),
      "submitted" -> JsNumber(submitted),
      "planned_logical_requests" -> JsNumber(request.planned_logical_requests))
  }

  private def emitC1BackendPressureStatus(runId: String,
                                          submitted: Int,
                                          completed: Int,
                                          failed: Int,
                                          notReady: Int): Unit = {
    logging.info(
      this,
      s"C1_BACKEND_PRESSURE_STATUS|run_id=${c1BackendPressureValue(runId)}|submitted=$submitted|completed=$completed|failed=$failed|not_ready=$notReady|mode=$c1BackendPressureMode")
  }

  private def emitC1BackendPressureResolvedAction(request: C1BackendPressureRequest,
                                                  action: WhiskActionMetaData): Unit = {
    val identity = C1BackendPressurePreparedRequests.actionIdentity(action)
    val fields = Seq(
      "run_id" -> request.run_id,
      "profile" -> request.profile,
      "request_generation_mode" -> request.request_generation_mode,
      "requested_action" -> request.action,
      "prepared_request_source" -> request.prepared_request_source.getOrElse(""),
      "resolved_canonical_fqen" -> identity.canonicalFqen.compactPrint,
      "resolved_action_revision" -> identity.revision,
      "evidence_phase" -> "premeasurement").map {
      case (key, value) => s"$key=${c1BackendPressureValue(value)}"
    }
    logging.info(this, s"C1_BACKEND_PRESSURE_RESOLVED_ACTION|${fields.mkString("|")}")
  }

  private def emitC1BackendPressurePreparedSubmit(request: C1BackendPressureRequest,
                                                  preparedRequest: C1BackendPressurePreparedRequest,
                                                  unixNs: Long,
                                                  monoNs: Long)(implicit transid: TransactionId): Unit = {
    val fields = C1BackendPressurePreparedRequests
      .submitEvidenceFields(
        request.run_id,
        preparedRequest,
        c1BackendPressureTimingNode,
        c1BackendPressureTimingProcessId,
        transid.id,
        unixNs,
        monoNs)
      .map {
        case (key, value) => s"$key=${c1BackendPressureValue(value)}"
      }
    logging.info(this, s"C1TIMING_EVENT|${fields.mkString("|")}")
  }

  private def emitC1BackendPressureStageDecision(request: C1BackendPressureRequest,
                                                 stageIndex: Int,
                                                 stageRatePerSec: Double,
                                                 stageStartOffsetNs: Long,
                                                 stageEndOffsetNs: Long,
                                                 stagePlannedRequests: Int,
                                                 stageSubmitted: Int,
                                                 stageSourceSubmitFailed: Int,
                                                 stageActiveAckFailed: Int,
                                                 stageActiveAckNotReady: Int,
                                                 sourceScheduleLagP50Ns: Long,
                                                 sourceScheduleLagP95Ns: Long,
                                                 sourceScheduleLagMaxNs: Long,
                                                 decision: String,
                                                 decisionReason: String,
                                                 nextStageIndex: Option[Int]): Unit = {
    val fields = Seq(
      "run_id" -> request.run_id,
      "request_generation_mode" -> request.request_generation_mode,
      "stage_gate_policy_id" -> request.stage_gate_policy_id.getOrElse(""),
      "ramp_stage_index" -> stageIndex.toString,
      "ramp_stage_rate_per_sec" -> stageRatePerSec.toString,
      "ramp_stage_start_offset_ns" -> stageStartOffsetNs.toString,
      "ramp_stage_end_offset_ns" -> stageEndOffsetNs.toString,
      "stage_planned_logical_requests" -> stagePlannedRequests.toString,
      "stage_submitted" -> stageSubmitted.toString,
      "stage_source_submit_failed" -> stageSourceSubmitFailed.toString,
      "stage_active_ack_failed" -> stageActiveAckFailed.toString,
      "stage_active_ack_not_ready" -> stageActiveAckNotReady.toString,
      "source_schedule_lag_p50_ns" -> sourceScheduleLagP50Ns.toString,
      "source_schedule_lag_p95_ns" -> sourceScheduleLagP95Ns.toString,
      "source_schedule_lag_max_ns" -> sourceScheduleLagMaxNs.toString,
      "decision" -> decision,
      "decision_reason" -> decisionReason,
      "next_ramp_stage_index" -> nextStageIndex.map(_.toString).getOrElse("")).map {
      case (key, value) => s"$key=${c1BackendPressureValue(value)}"
    }
    logging.info(this, s"C1_BACKEND_PRESSURE_STAGE_DECISION|${fields.mkString("|")}")
  }

  private def emitC1BackendPressureRunDecision(request: C1BackendPressureRequest,
                                               finalSubmittedLogicalRequests: Int,
                                               finalStageIndex: Int,
                                               finalDecision: String,
                                               finalDecisionReason: String,
                                               extraFields: Seq[(String, String)] = Seq.empty): Unit = {
    val fields = (Seq(
      "run_id" -> request.run_id,
      "request_generation_mode" -> request.request_generation_mode,
      "stage_gate_policy_id" -> request.stage_gate_policy_id.getOrElse(""),
      "candidate_planned_logical_requests" -> request.planned_logical_requests.toString,
      "final_submitted_logical_requests" -> finalSubmittedLogicalRequests.toString,
      "final_stage_index" -> finalStageIndex.toString,
      "final_decision" -> finalDecision,
      "final_decision_reason" -> finalDecisionReason) ++ extraFields).map {
      case (key, value) => s"$key=${c1BackendPressureValue(value)}"
    }
    logging.info(this, s"C1_BACKEND_PRESSURE_RUN_DECISION|${fields.mkString("|")}")
  }

  private def emitC1BackendPressureWindowEvent(request: C1BackendPressureRequest,
                                               logicalRequestId: String,
                                               submittedSoFar: Int,
                                               completedSoFar: Int,
                                               inFlightAfterCompletion: Int,
                                               nextLogicalRequestId: Option[Int],
                                               outcome: C1BackendPressureOutcome,
                                               completionElapsedNs: Option[Long] = None,
                                               rollingQps: Option[Double] = None,
                                               rollingWindowSize: Option[Int] = None,
                                               plateauDecision: Option[String] = None): Unit = {
    val (completionStatus, completionReason, activationId, attemptId) = outcome.result match {
      case Right(result) =>
        (result.status, result.reason, result.activationId.asString, result.attemptId.toString)
      case Left(reason) =>
        (BackendPressureActivationResult.Failed, reason, "", "")
    }
    val fields = Seq(
      "run_id" -> request.run_id,
      "request_generation_mode" -> request.request_generation_mode,
      "completion_window_size" -> request.completion_window_size.getOrElse(0).toString,
      "completion_signal" -> "blocking_activation_result",
      "terminal_audit" -> "blocking_activation_result_plus_N800_or_OW800_postrun",
      "logical_request_id" -> logicalRequestId,
      "attempt_id" -> attemptId,
      "activation_id" -> activationId,
      "submitted_so_far" -> submittedSoFar.toString,
      "completed_so_far" -> completedSoFar.toString,
      "inflight_after_completion" -> inFlightAfterCompletion.toString,
      "next_logical_request_id" -> nextLogicalRequestId.map(_.toString).getOrElse(""),
      "completion_status" -> completionStatus,
      "completion_reason" -> completionReason,
      "completion_elapsed_ns" -> completionElapsedNs.map(_.toString).getOrElse(""),
      "rolling_completion_qps" -> rollingQps.map(_.toString).getOrElse(""),
      "rolling_window_size" -> rollingWindowSize.map(_.toString).getOrElse(""),
      "plateau_decision" -> plateauDecision.getOrElse("")).map {
      case (key, value) => s"$key=${c1BackendPressureValue(value)}"
    }
    logging.info(this, s"C1_BACKEND_PRESSURE_WINDOW_EVENT|${fields.mkString("|")}")
  }

  private def c1BackendPressureStatusDetail(runId: String,
                                            logicalRequestId: String,
                                            status: String,
                                            reason: String,
                                            activationId: Option[ActivationId] = None,
                                            attemptId: Option[Int] = None)(
    implicit transid: TransactionId): Unit = {
    val fields = Seq(
      Some("run_id" -> runId),
      Some("logical_request_id" -> logicalRequestId),
      attemptId.map(id => "attempt_id" -> id.toString),
      activationId.map(id => "activation_id" -> id.asString),
      Some("status" -> status),
      Some("reason" -> reason),
      Some("mode" -> c1BackendPressureMode)).flatten.map {
      case (key, value) => s"$key=${c1BackendPressureValue(value)}"
    }
    logging.info(this, s"C1_BACKEND_PRESSURE_STATUS_DETAIL|${fields.mkString("|")}")
  }

  private def c1BackendPressureInvoke(user: Identity,
                                      action: WhiskActionMetaData,
                                      request: C1BackendPressureRequest,
                                      scheduleEntry: C1BackendPressureScheduleEntry,
                                      plannedSubmitMonoNs: Long,
                                      actualSubmitMonoNs: Long,
                                      sourceScheduleLagNs: Long,
                                      preparedPayload: Option[JsObject] = None,
                                      requireTerminalActivationResult: Boolean = false,
                                      schedulerFallbackRetryLimit: Int = 0)(
    implicit parentTransid: TransactionId): Future[C1BackendPressureOutcome] = {
    val logicalRequestIdString = scheduleEntry.logicalRequestId.toString
    val logicalTransid = TransactionId.childOf(parentTransid)
    val metadata = BackendPressureMetadata(
      runId = request.run_id,
      logicalRequestId = logicalRequestIdString,
      profile = request.profile,
      concurrency = request.concurrency,
      requestGenerationMode = request.request_generation_mode,
      targetArrivalRatePerSec = scheduleEntry.rampStageRatePerSec.orElse(request.target_arrival_rate_per_sec).getOrElse(0.0),
      durationSec = request.duration_sec.orElse(request.ramp_stage_duration_sec).getOrElse(0),
      plannedLogicalRequests = request.planned_logical_requests,
      rampStageIndex = scheduleEntry.rampStageIndex,
      rampStageRatePerSec = scheduleEntry.rampStageRatePerSec,
      rampStageStartOffsetNs = scheduleEntry.rampStageStartOffsetNs,
      rampStageEndOffsetNs = scheduleEntry.rampStageEndOffsetNs,
      plannedSubmitOffsetNs = Some(scheduleEntry.plannedSubmitOffsetNs),
      plannedSubmitMonoNs = plannedSubmitMonoNs,
      actualSubmitMonoNs = actualSubmitMonoNs,
      sourceScheduleLagNs = sourceScheduleLagNs)
    val payload = preparedPayload.getOrElse(c1BackendPressurePayload(request, scheduleEntry))

    val invocation =
      if (requireTerminalActivationResult) {
        invokeBackendPressureBlockingAction(
          user,
          action,
          Some(payload),
          metadata,
          schedulerFallbackRetryLimit)(logicalTransid)
      } else {
        invokeBackendPressureAction(user, action, Some(payload), metadata)(
          logicalTransid)
      }

    invocation
      .map { result =>
        if (!result.completed) {
          c1BackendPressureStatusDetail(
            request.run_id,
            logicalRequestIdString,
            result.status,
            result.reason,
            Some(result.activationId),
            Some(result.attemptId))(logicalTransid)
        }
        C1BackendPressureOutcome(logicalRequestIdString, Right(result), sourceScheduleLagNs)
      }
      .recover {
        case t: Throwable =>
          val reason = c1BackendPressureValue(t.getMessage)
          c1BackendPressureStatusDetail(
            request.run_id,
            logicalRequestIdString,
            BackendPressureActivationResult.Failed,
            reason)(logicalTransid)
          C1BackendPressureOutcome(logicalRequestIdString, Left(reason), sourceScheduleLagNs)
      }
  }

  private def waitUntilMonoNs(plannedSubmitMonoNs: Long): Future[Unit] = {
    val delayNs = plannedSubmitMonoNs - System.nanoTime()
    if (delayNs > 0) {
      org.apache.pekko.pattern.after(delayNs.nanos, actorSystem.scheduler)(Future.successful(()))
    } else {
      Future.successful(())
    }
  }

  private def c1BackendPressureOpenLoopRateSchedule(
    request: C1BackendPressureRequest): Vector[C1BackendPressureScheduleEntry] = {
    val rate = request.target_arrival_rate_per_sec.get
    (1 to request.planned_logical_requests).toVector.map { logicalRequestId =>
      C1BackendPressureScheduleEntry(
        logicalRequestId = logicalRequestId,
        plannedSubmitOffsetNs = math.floor((logicalRequestId - 1).toDouble * 1000000000.0 / rate).toLong)
    }
  }

  private def c1BackendPressureOpenLoopRampSchedule(
    request: C1BackendPressureRequest): Vector[C1BackendPressureScheduleEntry] = {
    val stageDurationSec = request.ramp_stage_duration_sec.get
    val stageDurationNs = stageDurationSec.toLong * 1000000000L
    val entries = Vector.newBuilder[C1BackendPressureScheduleEntry]
    var logicalRequestId = 1
    var stageStartOffsetNs = 0L

    request.ramp_rate_schedule_per_sec.get.zipWithIndex.foreach {
      case (rate, zeroBasedStageIndex) =>
        val stageIndex = zeroBasedStageIndex + 1
        val stageEndOffsetNs = stageStartOffsetNs + stageDurationNs
        val stagePlannedRequests = math.floor(rate * stageDurationSec).toInt
        (0 until stagePlannedRequests).foreach { requestIndexInStage =>
          val plannedSubmitOffsetNs =
            stageStartOffsetNs + math.floor(requestIndexInStage.toDouble * 1000000000.0 / rate).toLong
          entries += C1BackendPressureScheduleEntry(
            logicalRequestId = logicalRequestId,
            plannedSubmitOffsetNs = plannedSubmitOffsetNs,
            rampStageIndex = Some(stageIndex),
            rampStageRatePerSec = Some(rate),
            rampStageStartOffsetNs = Some(stageStartOffsetNs),
            rampStageEndOffsetNs = Some(stageEndOffsetNs))
          logicalRequestId += 1
        }
        stageStartOffsetNs = stageEndOffsetNs
    }

    entries.result()
  }

  private def runC1BackendPressureSchedule(user: Identity,
                                           action: WhiskActionMetaData,
                                           request: C1BackendPressureRequest,
                                           schedule: Vector[C1BackendPressureScheduleEntry])(
    implicit transid: TransactionId): Future[Vector[C1BackendPressureOutcome]] = {
    runC1BackendPressureScheduleFrom(System.nanoTime(), user, action, request, schedule)
  }

  private def runC1BackendPressureScheduleFrom(runStartMonoNs: Long,
                                               user: Identity,
                                               action: WhiskActionMetaData,
                                               request: C1BackendPressureRequest,
                                               schedule: Vector[C1BackendPressureScheduleEntry])(
    implicit transid: TransactionId): Future[Vector[C1BackendPressureOutcome]] = {
    val scheduled = schedule.map { scheduleEntry =>
      val plannedSubmitMonoNs = runStartMonoNs + scheduleEntry.plannedSubmitOffsetNs
      waitUntilMonoNs(plannedSubmitMonoNs).flatMap { _ =>
        val actualSubmitMonoNs = System.nanoTime()
        val sourceScheduleLagNs = math.max(0L, actualSubmitMonoNs - plannedSubmitMonoNs)
        c1BackendPressureInvoke(
          user,
          action,
          request,
          scheduleEntry,
          plannedSubmitMonoNs,
          actualSubmitMonoNs,
          sourceScheduleLagNs)
      }
    }
    Future.sequence(scheduled)
  }

  private def percentileNs(values: Vector[Long], percentile: Double): Long = {
    if (values.isEmpty) {
      0L
    } else {
      val sorted = values.sorted
      val index = math.max(0, math.ceil(percentile * sorted.size).toInt - 1)
      sorted(math.min(index, sorted.size - 1))
    }
  }

  private def stageGateFailureLimit(request: C1BackendPressureRequest, stagePlannedRequests: Int): Int =
    request.stage_gate_source_submit_failure_limit.getOrElse(math.max(100, math.floor(stagePlannedRequests * 0.01).toInt))

  private def stageGateDecisionReason(request: C1BackendPressureRequest,
                                      stagePlannedRequests: Int,
                                      stageSourceSubmitFailed: Int,
                                      sourceScheduleLagP95Ns: Long,
                                      sourceScheduleLagMaxNs: Long): Option[String] = {
    val sourceSubmitFailureLimit = stageGateFailureLimit(request, stagePlannedRequests)
    val p95LimitNs = math.round(request.stage_gate_source_schedule_lag_p95_ms.getOrElse(1000.0) * 1000000.0)
    val maxLimitNs = math.round(request.stage_gate_source_schedule_lag_max_ms.getOrElse(5000.0) * 1000000.0)
    if (stageSourceSubmitFailed > sourceSubmitFailureLimit) {
      Some(s"source_submit_failed_gt_$sourceSubmitFailureLimit")
    } else if (sourceScheduleLagP95Ns > p95LimitNs) {
      Some(s"source_schedule_lag_p95_gt_${p95LimitNs}_ns")
    } else if (sourceScheduleLagMaxNs > maxLimitNs) {
      Some(s"source_schedule_lag_max_gt_${maxLimitNs}_ns")
    } else {
      None
    }
  }

  private def runC1BackendPressureOpenLoopRate(user: Identity,
                                               action: WhiskActionMetaData,
                                               request: C1BackendPressureRequest)(
    implicit transid: TransactionId): Future[Vector[C1BackendPressureOutcome]] =
    runC1BackendPressureSchedule(user, action, request, c1BackendPressureOpenLoopRateSchedule(request))

  private def runC1BackendPressureOpenLoopRamp(user: Identity,
                                               action: WhiskActionMetaData,
                                               request: C1BackendPressureRequest)(
    implicit transid: TransactionId): Future[Vector[C1BackendPressureOutcome]] =
    runC1BackendPressureSchedule(user, action, request, c1BackendPressureOpenLoopRampSchedule(request))

  private def runC1BackendPressureStageGatedRamp(user: Identity,
                                                 action: WhiskActionMetaData,
                                                 request: C1BackendPressureRequest)(
    implicit transid: TransactionId): Future[Vector[C1BackendPressureOutcome]] = {
    val runStartMonoNs = System.nanoTime()
    val stages = c1BackendPressureOpenLoopRampSchedule(request)
      .groupBy(_.rampStageIndex.getOrElse(0))
      .toVector
      .map { case (stageIndex, entries) => stageIndex -> entries.sortBy(_.logicalRequestId) }
      .sortBy(_._1)

    def loop(remaining: Vector[(Int, Vector[C1BackendPressureScheduleEntry])],
             accumulated: Vector[C1BackendPressureOutcome]): Future[Vector[C1BackendPressureOutcome]] = {
      remaining.headOption match {
        case None =>
          emitC1BackendPressureRunDecision(request, accumulated.size, 0, "complete", "no_stages")
          Future.successful(accumulated)
        case Some((stageIndex, stageEntries)) =>
          runC1BackendPressureScheduleFrom(runStartMonoNs, user, action, request, stageEntries).flatMap { stageResults =>
            val stagePlannedRequests = stageEntries.size
            val stageSubmitted = stageResults.size
            val stageSourceSubmitFailed = stageResults.count(_.result.isLeft)
            val stageActiveAckFailed = stageResults.count {
              case C1BackendPressureOutcome(_, Right(result), _) => result.failed
              case _                                             => false
            }
            val stageActiveAckNotReady = stageResults.count {
              case C1BackendPressureOutcome(_, Right(result), _) => result.notReady
              case _                                             => false
            }
            val lags = stageResults.map(_.sourceScheduleLagNs)
            val lagP50 = percentileNs(lags, 0.50)
            val lagP95 = percentileNs(lags, 0.95)
            val lagMax = if (lags.isEmpty) 0L else lags.max
            val stopReason = stageGateDecisionReason(
              request,
              stagePlannedRequests,
              stageSourceSubmitFailed,
              lagP95,
              lagMax)
            val isLastStage = remaining.size == 1
            val decision = if (stopReason.nonEmpty || isLastStage) "stop" else "continue"
            val decisionReason = stopReason.getOrElse(if (isLastStage) "candidate_schedule_complete" else "source_lag_v1_ok")
            val nextStageIndex = if (decision == "continue") remaining.drop(1).headOption.map(_._1) else None
            val firstEntry = stageEntries.head
            emitC1BackendPressureStageDecision(
              request,
              stageIndex,
              firstEntry.rampStageRatePerSec.getOrElse(0.0),
              firstEntry.rampStageStartOffsetNs.getOrElse(0L),
              firstEntry.rampStageEndOffsetNs.getOrElse(0L),
              stagePlannedRequests,
              stageSubmitted,
              stageSourceSubmitFailed,
              stageActiveAckFailed,
              stageActiveAckNotReady,
              lagP50,
              lagP95,
              lagMax,
              decision,
              decisionReason,
              nextStageIndex)

            val nextAccumulated = accumulated ++ stageResults
            if (decision == "continue") {
              loop(remaining.drop(1), nextAccumulated)
            } else {
              val finalDecision =
                if (stopReason.nonEmpty) "stopped_by_gate" else "complete"
              emitC1BackendPressureRunDecision(request, nextAccumulated.size, stageIndex, finalDecision, decisionReason)
              Future.successful(nextAccumulated)
            }
          }
      }
    }

    loop(stages, Vector.empty)
  }

  private def runC1BackendPressureCompletionWindow(user: Identity,
                                                   action: WhiskActionMetaData,
                                                   request: C1BackendPressureRequest,
                                                   preparedRequests: Option[Vector[C1BackendPressurePreparedRequest]])(
    implicit transid: TransactionId): Future[Vector[C1BackendPressureOutcome]] = {
    val runStartMonoNs = System.nanoTime()
    val targetLogicalRequests = request.planned_logical_requests
    val windowSize = math.min(request.completion_window_size.getOrElse(1), targetLogicalRequests)
    val completionWindowTimeoutSec = request.completion_window_timeout_sec.get
    val deadlineMonoNs = C1BackendPressureCompletionWindowTransitions.completionWindowDeadlineMonoNs(
      runStartMonoNs,
      completionWindowTimeoutSec)
    val deadlineDelayNs = C1BackendPressureCompletionWindowTransitions.remainingDeadlineNs(
      deadlineMonoNs,
      System.nanoTime())
    val deadlineSignal =
      org.apache.pekko.pattern.after(deadlineDelayNs.nanos, actorSystem.scheduler)(Future.successful(()))
    val plateauEnabled = request.plateau_policy_enabled.getOrElse(false)
    val plateauWindowSize = math.min(request.plateau_window_size.getOrElse(windowSize), targetLogicalRequests)
    val plateauWarmupCompletions = request.plateau_warmup_completions.getOrElse(1000)
    val plateauWarmupNs = request.plateau_warmup_sec.getOrElse(10).toLong * 1000000000L
    val plateauNoImproveNs = request.plateau_no_improve_sec.getOrElse(10).toLong * 1000000000L
    val plateauMinImprovementFraction = request.plateau_min_improvement_fraction.getOrElse(0.03)
    val schedulerFallbackRetryEnabled = request.scheduler_fallback_retry_enabled.getOrElse(false)
    val schedulerFallbackRetryLimit =
      if (schedulerFallbackRetryEnabled) request.scheduler_fallback_retry_limit.getOrElse(0) else 0

    def plateauRunDecisionFields(state: C1BackendPressurePlateauState,
                                 completionCount: Int,
                                 elapsedNs: Long): Seq[(String, String)] =
      Seq(
        "plateau_policy_enabled" -> plateauEnabled.toString,
        "plateau_window_size" -> plateauWindowSize.toString,
        "plateau_warmup_completions" -> plateauWarmupCompletions.toString,
        "plateau_warmup_sec" -> request.plateau_warmup_sec.getOrElse(10).toString,
        "plateau_no_improve_sec" -> request.plateau_no_improve_sec.getOrElse(10).toString,
        "plateau_min_improvement_fraction" -> plateauMinImprovementFraction.toString,
        "plateau_completion_count" -> completionCount.toString,
        "plateau_elapsed_ns" -> elapsedNs.toString,
        "plateau_best_rolling_qps" -> state.bestRollingQps.toString,
        "plateau_current_rolling_qps" -> state.currentRollingQps.toString,
        "plateau_best_completion_elapsed_ns" -> state.bestCompletionElapsedNs.toString,
        "scheduler_fallback_retry_enabled" -> schedulerFallbackRetryEnabled.toString,
        "scheduler_fallback_retry_limit" -> schedulerFallbackRetryLimit.toString)

    def launch(logicalRequestId: Int, actualSubmitMonoNs: Long): (Int, Future[C1BackendPressureOutcome]) = {
      val plannedSubmitOffsetNs = math.max(0L, actualSubmitMonoNs - runStartMonoNs)
      val scheduleEntry = C1BackendPressureScheduleEntry(
        logicalRequestId = logicalRequestId,
        plannedSubmitOffsetNs = plannedSubmitOffsetNs)
      preparedRequests.foreach { requests =>
        emitC1BackendPressurePreparedSubmit(
          request,
          requests(logicalRequestId - 1),
          System.currentTimeMillis() * 1000000L,
          actualSubmitMonoNs)
      }
      logicalRequestId -> c1BackendPressureInvoke(
        user,
        action,
        request,
        scheduleEntry,
        actualSubmitMonoNs,
        actualSubmitMonoNs,
        0L,
        preparedPayload = preparedRequests.map(_(logicalRequestId - 1).actionParams),
        requireTerminalActivationResult = true,
        schedulerFallbackRetryLimit = schedulerFallbackRetryLimit)
    }

    def timeoutRunResult(nextLogicalRequestId: Int,
                         inFlight: Vector[(Int, Future[C1BackendPressureOutcome])],
                         accumulated: Vector[C1BackendPressureOutcome],
                         plateauState: C1BackendPressurePlateauState,
                         observedMonoNs: Long): Future[Vector[C1BackendPressureOutcome]] = {
      val observedElapsedNs = math.max(0L, observedMonoNs - runStartMonoNs)
      emitC1BackendPressureRunDecision(
        request,
        nextLogicalRequestId - 1,
        0,
        "timeout",
        "completion_window_timeout",
        plateauRunDecisionFields(plateauState, accumulated.size, observedElapsedNs) ++ Seq(
          "completion_window_timeout_sec" -> completionWindowTimeoutSec.toString,
          "completion_window_deadline_elapsed_ns" -> (deadlineMonoNs - runStartMonoNs).toString,
          "completion_window_timeout_observed_elapsed_ns" -> observedElapsedNs.toString,
          "completion_window_completed_at_timeout" -> accumulated.size.toString,
          "completion_window_inflight_at_timeout" -> inFlight.size.toString,
          "completion_window_timeout_classification" -> "invalid",
          "completion_window_inflight_disposition" -> "not_cancelled_no_refill"))
      val timedOutInFlight = inFlight.map {
        case (logicalRequestId, _) =>
          C1BackendPressureOutcome(logicalRequestId.toString, Left("completion_window_timeout_inflight"), 0L)
      }
      Future.successful(accumulated ++ timedOutInFlight)
    }

    def loop(nextLogicalRequestId: Int,
             inFlight: Vector[(Int, Future[C1BackendPressureOutcome])],
             accumulated: Vector[C1BackendPressureOutcome],
             completionTimesNs: Vector[Long],
             plateauState: C1BackendPressurePlateauState,
             stopDecision: Option[(String, String, C1BackendPressurePlateauState, Int, Long)]): Future[Vector[C1BackendPressureOutcome]] = {
      if (inFlight.isEmpty) {
        stopDecision match {
          case Some((decision, reason, finalPlateauState, finalCompletionCount, finalElapsedNs)) =>
            emitC1BackendPressureRunDecision(
              request,
              accumulated.size,
              0,
              decision,
              reason,
              plateauRunDecisionFields(finalPlateauState, finalCompletionCount, finalElapsedNs))
          case None =>
            val reason =
              if (plateauEnabled && accumulated.size < targetLogicalRequests) "completion_window_plateau_stop"
              else "completion_window_target_complete"
            emitC1BackendPressureRunDecision(
              request,
              accumulated.size,
              0,
              "complete",
              reason,
              plateauRunDecisionFields(plateauState, accumulated.size, completionTimesNs.lastOption.getOrElse(0L)))
        }
        Future.successful(accumulated)
      } else {
        C1BackendPressureCompletionWindowTransitions
          .firstCompletionOrDeadline(
            Future.firstCompletedOf(inFlight.map(_._2)),
            deadlineSignal,
            deadlineMonoNs,
            () => System.nanoTime())
          .flatMap {
            case C1BackendPressureCompletionWindowDeadlineReached(observedMonoNs) =>
              timeoutRunResult(nextLogicalRequestId, inFlight, accumulated, plateauState, observedMonoNs)
            case C1BackendPressureCompletionBeforeDeadline(outcome, completionMonoNs) =>
              val completionElapsedNs = math.max(0L, completionMonoNs - runStartMonoNs)
              val remaining = inFlight.filterNot { case (logicalRequestId, _) =>
                logicalRequestId.toString == outcome.logicalRequestId
              }
              val completedSoFar = accumulated.size + 1
              val terminalResult = outcome.result match {
                case Right(result) => result.completed || result.failed
                case Left(_)       => false
              }
              val evaluatedPlateauState =
                if (terminalResult) {
                  C1BackendPressureCompletionWindowTransitions.evaluatePlateauCompletion(
                    previous = plateauState,
                    completionTimesNs = completionTimesNs,
                    completionElapsedNs = completionElapsedNs,
                    plateauEnabled = plateauEnabled,
                    plateauWindowSize = plateauWindowSize,
                    plateauWarmupCompletions = plateauWarmupCompletions,
                    plateauWarmupNs = plateauWarmupNs,
                    plateauNoImproveNs = plateauNoImproveNs,
                    plateauMinImprovementFraction = plateauMinImprovementFraction)
                } else {
                  plateauState
                }
              val plateauStopNow = terminalResult && evaluatedPlateauState.stop
              val targetReached = nextLogicalRequestId > targetLogicalRequests
              val refillSubmitMonoNs = System.nanoTime()
              val deadlineReachedBeforeRefill = C1BackendPressureCompletionWindowTransitions.deadlineReached(
                deadlineMonoNs,
                refillSubmitMonoNs)
              val terminalTransition =
                if (terminalResult) {
                  Some(
                    C1BackendPressureCompletionWindowTransitions.afterTerminalCompletion(
                      nextLogicalRequestId,
                      remaining.size,
                      targetLogicalRequests,
                      plateauStopNow,
                      stopDecision.exists(_._3.stop),
                      refillAllowed = !deadlineReachedBeforeRefill))
                } else {
                  None
                }
              val newNextLogicalRequestId =
                terminalTransition.map(_.nextLogicalRequestId).getOrElse(nextLogicalRequestId)
              val refillLogicalRequestId = terminalTransition.flatMap(_.refillLogicalRequestId)
              val nextInFlight =
                refillLogicalRequestId
                  .map(logicalRequestId => remaining :+ launch(logicalRequestId, refillSubmitMonoNs))
                  .getOrElse {
                    if (terminalResult) remaining else Vector.empty
                  }
              val stopLatched = terminalTransition.exists(_.stopLatched)
              val latchedStopReason =
                stopDecision.map(_._3.stopReason).filter(_.nonEmpty).getOrElse(evaluatedPlateauState.stopReason)
              val newPlateauState =
                if (stopLatched) evaluatedPlateauState.copy(stop = true, stopReason = latchedStopReason)
                else evaluatedPlateauState
              val newStopDecision = stopDecision.orElse {
                if (plateauStopNow) {
                  Some(
                    ("complete", "completion_window_plateau_stop", newPlateauState, completedSoFar, completionElapsedNs))
                } else if (terminalResult && targetReached && nextInFlight.isEmpty) {
                  Some(
                    ("complete", "completion_window_target_complete", newPlateauState, completedSoFar, completionElapsedNs))
                } else {
                  None
                }
              }
              emitC1BackendPressureWindowEvent(
                request,
                outcome.logicalRequestId,
                submittedSoFar = newNextLogicalRequestId - 1,
                completedSoFar = completedSoFar,
                inFlightAfterCompletion = nextInFlight.size,
                nextLogicalRequestId = refillLogicalRequestId,
                outcome = outcome,
                completionElapsedNs = if (terminalResult) Some(completionElapsedNs) else None,
                rollingQps =
                  if (newPlateauState.currentRollingQps > 0.0) Some(newPlateauState.currentRollingQps) else None,
                rollingWindowSize =
                  if (newPlateauState.rollingWindowSize > 0) Some(newPlateauState.rollingWindowSize) else None,
                plateauDecision = if (stopLatched) Some("stop") else if (plateauEnabled) Some("continue") else None)
              if (terminalResult) {
                val newAccumulated = accumulated :+ outcome
                val newCompletionTimesNs = completionTimesNs :+ completionElapsedNs
                if (deadlineReachedBeforeRefill) {
                  timeoutRunResult(
                    newNextLogicalRequestId,
                    nextInFlight,
                    newAccumulated,
                    newPlateauState,
                    refillSubmitMonoNs)
                } else {
                  loop(
                    newNextLogicalRequestId,
                    nextInFlight,
                    newAccumulated,
                    newCompletionTimesNs,
                    newPlateauState,
                    newStopDecision)
                }
              } else {
                val reason = outcome.result match {
                  case Right(result) => result.reason
                  case Left(error)   => error
                }
                emitC1BackendPressureRunDecision(
                  request,
                  newNextLogicalRequestId - 1,
                  0,
                  "invalid_non_terminal_completion_signal",
                  reason,
                  plateauRunDecisionFields(newPlateauState, completedSoFar, completionElapsedNs))
                Future.successful(accumulated :+ outcome)
              }
          }
      }
    }

    val initialBuilder = Vector.newBuilder[(Int, Future[C1BackendPressureOutcome])]
    var nextInitialLogicalRequestId = 1
    var initialDeadlineObserved = Option.empty[Long]
    while (nextInitialLogicalRequestId <= windowSize && initialDeadlineObserved.isEmpty) {
      val submitMonoNs = System.nanoTime()
      if (C1BackendPressureCompletionWindowTransitions.deadlineReached(deadlineMonoNs, submitMonoNs)) {
        initialDeadlineObserved = Some(submitMonoNs)
      } else {
        initialBuilder += launch(nextInitialLogicalRequestId, submitMonoNs)
        nextInitialLogicalRequestId += 1
      }
    }
    val initial = initialBuilder.result()
    val initialPlateauState = C1BackendPressurePlateauState(rollingWindowSize = plateauWindowSize)
    initialDeadlineObserved match {
      case Some(observedMonoNs) =>
        timeoutRunResult(nextInitialLogicalRequestId, initial, Vector.empty, initialPlateauState, observedMonoNs)
      case None =>
        loop(nextInitialLogicalRequestId, initial, Vector.empty, Vector.empty, initialPlateauState, None)
    }
  }

  def backendPressureRoutes(user: Identity)(implicit transid: TransactionId) = {
    (path("c1" / "backend-pressure") & post) {
      if (!c1BackendPressureControllerSourceEnabled) {
        terminate(
          Forbidden,
          s"C1 backend-pressure controller source is not enabled; set $c1BackendPressureControllerSourceEnv=1")
      } else {
        entity(as[C1BackendPressureRequest]) { request =>
          validateC1BackendPressureRequest(request) match {
            case Some(error) =>
              terminate(BadRequest, error)
            case None =>
              val namespace = Try(EntityName(request.namespace))
              val entityName = namespace.toOption.flatMap { ns =>
                FullyQualifiedEntityName.resolveName(JsString(request.action), ns)
              }

              entityName match {
                case None =>
                  terminate(BadRequest, "namespace or action is malformed")
                case Some(name) =>
                  getEntity(WhiskActionMetaData.resolveActionAndMergeParameters(entityStore, name), Some {
                    actionMetaData: WhiskActionMetaData =>
                      val action = actionMetaData.resolve(user.namespace)
                      val resource = Resource(name.path, collection, Some(name.name.asString))
                      val checks = for {
                        _ <- entitlementProvider.check(user, Privilege.ACTIVATE, resource)
                        _ <- entitleReferencedEntitiesMetaData(user, Privilege.ACTIVATE, Some(action.exec))
                      } yield ()

                      onComplete(checks) {
                        case Success(_) =>
                          val preparedRequestResult =
                            if (
                              Set("asyncs", "reusable-concurrency").contains(request.profile) &&
                              request.request_generation_mode == c1BackendPressureRequestModeCompletionWindow) {
                              C1BackendPressurePreparedRequests
                                .load(
                                  request.prepared_request_source.get,
                                  request.planned_logical_requests,
                                  request.workload_id,
                                  request.profile)
                                .map(requests => Some(requests))
                            } else {
                              Right(None)
                            }
                          preparedRequestResult match {
                            case Left(error) =>
                              terminate(BadRequest, s"prepared request source is invalid: $error")
                            case Right(preparedRequests) =>
                              preparedRequests.foreach(_ => emitC1BackendPressureResolvedAction(request, action))
                              val submitted = request.planned_logical_requests
                              if (
                                request.request_generation_mode == c1BackendPressureRequestModeRamp ||
                                request.request_generation_mode == c1BackendPressureRequestModeStageGatedRamp ||
                                request.request_generation_mode == c1BackendPressureRequestModeCompletionWindow) {
                                val run =
                                  if (request.request_generation_mode == c1BackendPressureRequestModeStageGatedRamp) {
                                    runC1BackendPressureStageGatedRamp(user, action, request)
                                  } else if (request.request_generation_mode == c1BackendPressureRequestModeCompletionWindow) {
                                    runC1BackendPressureCompletionWindow(user, action, request, preparedRequests)
                                  } else {
                                    runC1BackendPressureOpenLoopRamp(user, action, request)
                                  }
                                run.onComplete {
                                  case Success(results) =>
                                    val completed = results.count {
                                      case C1BackendPressureOutcome(_, Right(result), _) => result.completed
                                      case _                                          => false
                                    }
                                    val failed = results.count {
                                      case C1BackendPressureOutcome(_, Right(result), _) => result.failed
                                      case C1BackendPressureOutcome(_, Left(_), _)       => true
                                    }
                                    val notReady = results.count {
                                      case C1BackendPressureOutcome(_, Right(result), _) => result.notReady
                                      case _                                          => false
                                    }
                                    val finalSubmitted =
                                      if (
                                        request.request_generation_mode == c1BackendPressureRequestModeStageGatedRamp ||
                                        request.request_generation_mode == c1BackendPressureRequestModeCompletionWindow) {
                                        results.size
                                      } else {
                                        submitted
                                      }
                                    emitC1BackendPressureStatus(request.run_id, finalSubmitted, completed, failed, notReady)
                                  case Failure(_) =>
                                    if (
                                      request.request_generation_mode == c1BackendPressureRequestModeStageGatedRamp ||
                                      request.request_generation_mode == c1BackendPressureRequestModeCompletionWindow) {
                                      emitC1BackendPressureRunDecision(request, 0, 0, "aborted", "controller_background_failure")
                                    }
                                    emitC1BackendPressureStatus(request.run_id, submitted, 0, submitted, 0)
                                }
                                complete(Accepted, c1BackendPressureAckResponse(request, submitted))
                              } else {
                                val run = runC1BackendPressureOpenLoopRate(user, action, request)
                                onComplete(run) {
                                  case Success(results) =>
                                    val completed = results.count {
                                      case C1BackendPressureOutcome(_, Right(result), _) => result.completed
                                      case _                                          => false
                                    }
                                    val failed = results.count {
                                      case C1BackendPressureOutcome(_, Right(result), _) => result.failed
                                      case C1BackendPressureOutcome(_, Left(_), _)       => true
                                    }
                                    val notReady = results.count {
                                      case C1BackendPressureOutcome(_, Right(result), _) => result.notReady
                                      case _                                          => false
                                    }
                                    emitC1BackendPressureStatus(request.run_id, submitted, completed, failed, notReady)
                                    complete(
                                      OK,
                                      c1BackendPressureResponse(
                                        request.run_id,
                                        request.request_generation_mode,
                                        submitted,
                                        completed,
                                        failed,
                                        notReady,
                                        results))
                                  case Failure(t) =>
                                    emitC1BackendPressureStatus(request.run_id, submitted, 0, submitted, 0)
                                    terminate(InternalServerError, t.getMessage)
                                }
                              }
                          }

                        case Failure(f) =>
                          super.handleEntitlementFailure(f)
                      }
                  })
              }
          }
        }
      }
    }
  }

  /**
   * Handles operations on action resources, which encompass these cases:
   *
   * 1. ns/foo     -> subject must be authorized for one of { action(ns, *), action(ns, foo) },
   *                  resource resolves to { action(ns, foo) }
   *
   * 2. ns/bar/foo -> where bar is a package
   *                  subject must be authorized for one of { package(ns, *), package(ns, bar), action(ns.bar, foo) }
   *                  resource resolves to { action(ns.bar, foo) }
   *
   * 3. ns/baz/foo -> where baz is a binding to ns'.bar
   *                  subject must be authorized for one of { package(ns, *), package(ns, baz) }
   *                  *and* one of { package(ns', *), package(ns', bar), action(ns'.bar, foo) }
   *                  resource resolves to { action(ns'.bar, foo) }
   *
   * Note that package(ns, xyz) == action(ns.xyz, *) and if subject has rights to package(ns, xyz)
   * then they also have rights to action(ns.xyz, *) since sharing is done at the package level and
   * is not more granular; hence a check on action(ns.xyz, abc) is eschewed.
   *
   * Only list is supported for these resources:
   *
   * 4. ns/bar/    -> where bar is a package
   *                  subject must be authorized for one of { package(ns, *), package(ns, bar) }
   *                  resource resolves to { action(ns.bar, *) }
   *
   * 5. ns/baz/    -> where baz is a binding to ns'.bar
   *                  subject must be authorized for one of { package(ns, *), package(ns, baz) }
   *                  *and* one of { package(ns', *), package(ns', bar) }
   *                  resource resolves to { action(ns.bar, *) }
   */
  protected override def innerRoutes(user: Identity, ns: EntityPath)(implicit transid: TransactionId) = {
    (entityPrefix & entityOps & requestMethod) { (segment, m) =>
      entityname(segment) { outername =>
        pathEnd {
          // matched /namespace/collection/name
          // this is an action in default package, authorize and dispatch
          authorizeAndDispatch(m, user, Resource(ns, collection, Some(outername)))
        } ~ (get & pathSingleSlash) {
          // matched GET /namespace/collection/package-name/
          // list all actions in package iff subject is entitled to READ package
          val resource = Resource(ns, Collection(Collection.PACKAGES), Some(outername))
          onComplete(entitlementProvider.check(user, Privilege.READ, resource)) {
            case Success(_) => listPackageActions(user, FullyQualifiedEntityName(ns, EntityName(outername)))
            case Failure(f) => super.handleEntitlementFailure(f)
          }
        } ~ (entityPrefix & pathEnd) { segment =>
          entityname(segment) { innername =>
            // matched /namespace/collection/package-name/action-name
            // this is an action in a named package
            val packageDocId = FullyQualifiedEntityName(ns, EntityName(outername)).toDocId
            val packageResource = Resource(ns.addPath(EntityName(outername)), collection, Some(innername))

            val right = collection.determineRight(m, Some(innername))
            onComplete(entitlementProvider.check(user, right, packageResource)) {
              case Success(_) =>
                getEntity(WhiskPackage.get(entityStore, packageDocId), Some {
                  if (right == Privilege.READ || right == Privilege.ACTIVATE) { wp: WhiskPackage =>
                    val actionResource = Resource(wp.fullPath, collection, Some(innername))
                    dispatchOp(user, right, actionResource)

                  } else {
                    // these packaged action operations do not need merging with the package,
                    // but may not be permitted if this is a binding, or if the subject does
                    // not have PUT and DELETE rights to the package itself
                    (wp: WhiskPackage) =>
                      wp.binding map { _ =>
                        terminate(BadRequest, Messages.notAllowedOnBinding)
                      } getOrElse {
                        val actionResource = Resource(wp.fullPath, collection, Some(innername))
                        dispatchOp(user, right, actionResource)
                      }
                  }
                })
              case Failure(f) => super.handleEntitlementFailure(f)
            }
          }
        }
      }
    }
  }

  /**
   * Creates or updates action if it already exists. The PUT content is deserialized into a WhiskActionPut
   * which is a subset of WhiskAction (it eschews the namespace and entity name since the former is derived
   * from the authenticated user and the latter is derived from the URI). The WhiskActionPut is merged with
   * the existing WhiskAction in the datastore, overriding old values with new values that are defined.
   * Any values not defined in the PUT content are replaced with old values.
   *
   * Responses are one of (Code, Message)
   * - 200 WhiskAction as JSON
   * - 400 Bad Request
   * - 409 Conflict
   * - 500 Internal Server Error
   */
  override def create(user: Identity, entityName: FullyQualifiedEntityName)(implicit transid: TransactionId) = {
    parameter('overwrite ? false) { overwrite =>
      entity(as[WhiskActionPut]) { content =>
        val request = content.resolve(user.namespace)
        val check = for {
          checkLimits <- checkActionLimits(user, content)
          checkAdditionalPrivileges <- entitleReferencedEntities(user, Privilege.READ, request.exec).flatMap(_ =>
            entitlementProvider.check(user, content.exec))
        } yield (checkAdditionalPrivileges, checkLimits)

        onComplete(check) {
          case Success(_) =>
            putEntity(WhiskAction, entityStore, entityName.toDocId, overwrite, update(user, request), () => {
              make(user, entityName, request)
            })
          case Failure(f) =>
            super.handleEntitlementFailure(f)
        }
      }
    }
  }

  /**
   * Invokes action if it exists. The POST content is deserialized into a Payload and posted
   * to the loadbalancer.
   *
   * Responses are one of (Code, Message)
   * - 200 Activation as JSON if blocking or just the result JSON iff '&result=true'
   * - 202 ActivationId as JSON (this is issued on non-blocking activation or blocking activation that times out)
   * - 404 Not Found
   * - 502 Bad Gateway
   * - 500 Internal Server Error
   */
  override def activate(user: Identity, entityName: FullyQualifiedEntityName, env: Option[Parameters])(
    implicit transid: TransactionId) = {
    parameter(
      'blocking ? false,
      'result ? false,
      'timeout.as[FiniteDuration] ? controllerActivationConfig.maxWaitForBlockingActivation) {
      (blocking, result, waitOverride) =>
        entity(as[Option[JsObject]]) { payload =>
          getEntity(WhiskActionMetaData.resolveActionAndMergeParameters(entityStore, entityName), Some {
            act: WhiskActionMetaData =>
              // resolve the action --- special case for sequences that may contain components with '_' as default package
              val action = act.resolve(user.namespace)
              onComplete(entitleReferencedEntitiesMetaData(user, Privilege.ACTIVATE, Some(action.exec))) {
                case Success(_) =>
                  val actionWithMergedParams = env.map(action.inherit(_)) getOrElse action

                  // incoming parameters may not override final parameters (i.e., parameters with already defined values)
                  // on an action once its parameters are resolved across package and binding
                  val allowInvoke = payload
                    .map(_.fields.keySet.forall(key => !actionWithMergedParams.immutableParameters.contains(key)))
                    .getOrElse(true)

                  val isConfidential = actionWithMergedParams.annotations.isTruthy("confidential")
                  val confidentialCheck = if (isConfidential) {
                    payload match {
                      case Some(p) =>
                        val fields = p.fields
                        val fid = fields.get("FID").flatMap {
                          case JsString(s) => Some(s)
                          case _ => None
                        }
                        val cReq = fields.contains("C_req")
                        val cKey = fields.contains("C_key")
                        val pkU = fields.contains("pkU")
                        val nonce = fields.contains("nonce")

                        val actionFid = actionWithMergedParams.annotations.getAs[String]("FID").toOption

                        (fid, actionFid) match {
                          case (Some(f), Some(af)) => f == af && cReq && cKey && pkU && nonce
                          case _ => false
                        }
                      case None => false
                    }
                  } else true

                  if (allowInvoke && confidentialCheck) {
                    doInvoke(user, actionWithMergedParams, payload, blocking, waitOverride, result)
                  } else if (!allowInvoke) {
                    terminate(BadRequest, Messages.parametersNotAllowed)
                  } else {
                    terminate(BadRequest, "Confidential action invocation failed: missing or invalid FID, C_req, C_key, pkU, or nonce.")
                  }

                case Failure(f) =>
                  super.handleEntitlementFailure(f)
              }
          })
        }
    }
  }

  private def doInvoke(user: Identity,
                       actionWithMergedParams: WhiskActionMetaData,
                       payload: Option[JsObject],
                       blocking: Boolean,
                       waitOverride: FiniteDuration,
                       result: Boolean)(implicit transid: TransactionId): RequestContext => Future[RouteResult] = {
    val waitForResponse = if (blocking) Some(waitOverride) else None
    onComplete(invokeAction(user, actionWithMergedParams, payload, waitForResponse, cause = None)) {
      case Success(Left(activationId)) =>
        // non-blocking invoke or blocking invoke which got queued instead
        respondWithActivationIdHeader(activationId) {
          complete(Accepted, activationId.toJsObject)
        }
      case Success(Right(activation)) =>
        val response = activation.response.result match {
          case Some(JsArray(elements)) =>
            JsArray(elements)
          case _ =>
            if (result) activation.resultAsJson else activation.toExtendedJson()
        }
        respondWithActivationIdHeader(activation.activationId) {
          if (activation.response.isSuccess) {
            complete(OK, response)
          } else if (activation.response.isApplicationError) {
            // actions that result is ApplicationError status are considered a 'success'
            // and will have an 'error' property in the result - the HTTP status is OK
            // and clients must check the response status if it exists
            // NOTE: response status will not exist in the JSON object if ?result == true
            // and instead clients must check if 'error' is in the JSON
            // PRESERVING OLD BEHAVIOR and will address defect in separate change
            complete(BadGateway, response)
          } else if (activation.response.isContainerError) {
            complete(BadGateway, response)
          } else {
            complete(InternalServerError, response)
          }
        }
      case Failure(t: RecordTooLargeException) =>
        logging.debug(this, s"[POST] action payload was too large")
        terminate(ContentTooLarge)
      case Failure(RejectRequest(code, message)) =>
        logging.debug(this, s"[POST] action rejected with code $code: $message")
        terminate(code, message)
      case Failure(t: LoadBalancerException) =>
        logging.error(this, s"[POST] failed in loadbalancer: ${t.getMessage}")
        terminate(ServiceUnavailable)
      case Failure(t: Throwable) =>
        logging.error(this, s"[POST] action activation failed: ${t.getMessage}")
        terminate(InternalServerError)
    }
  }

  /**
   * Deletes action.
   *
   * Responses are one of (Code, Message)
   * - 200 WhiskAction as JSON
   * - 404 Not Found
   * - 409 Conflict
   * - 500 Internal Server Error
   */
  override def remove(user: Identity, entityName: FullyQualifiedEntityName)(implicit transid: TransactionId) = {
    deleteEntity(WhiskAction, entityStore, entityName.toDocId, (a: WhiskAction) => Future.successful({}))
  }

  /** Checks for package binding case. we don't want to allow get for a package binding in shared package */
  private def fetchEntity(entityName: FullyQualifiedEntityName, env: Option[Parameters], code: Boolean)(
    implicit transid: TransactionId) = {
    val resolvedPkg: Future[Either[String, FullyQualifiedEntityName]] = if (entityName.path.defaultPackage) {
      Future.successful(Right(entityName))
    } else {
      WhiskPackage.resolveBinding(entityStore, entityName.path.toDocId, mergeParameters = true).map { pkg =>
        val originalPackageLocation = pkg.fullyQualifiedName(withVersion = false).namespace
        if (executeOnly && originalPackageLocation != entityName.namespace) {
          Left(forbiddenGetActionBinding(entityName.toDocId.asString))
        } else {
          Right(entityName)
        }
      }
    }
    onComplete(resolvedPkg) {
      case Success(pkgFuture) =>
        pkgFuture match {
          case Left(f) => terminate(Forbidden, f)
          case Right(_) =>
            if (code) {
              getEntity(WhiskAction.resolveActionAndMergeParameters(entityStore, entityName), Some {
                action: WhiskAction =>
                  val mergedAction = env map {
                    action inherit _
                  } getOrElse action
                  complete(OK, mergedAction)
              })
            } else {
              getEntity(WhiskActionMetaData.resolveActionAndMergeParameters(entityStore, entityName), Some {
                action: WhiskActionMetaData =>
                  val mergedAction = env map {
                    action inherit _
                  } getOrElse action
                  complete(OK, mergedAction)
              })
            }
        }
      case Failure(t: Throwable) =>
        logging.error(this, s"[GET] package ${entityName.path.toDocId} failed: ${t.getMessage}")
        terminate(InternalServerError)
    }
  }

  /**
   * Gets action. The action name is prefixed with the namespace to create the primary index key.
   *
   * Responses are one of (Code, Message)
   * - 200 WhiskAction has JSON
   * - 404 Not Found
   * - 500 Internal Server Error
   */
  override def fetch(user: Identity, entityName: FullyQualifiedEntityName, env: Option[Parameters])(
    implicit transid: TransactionId) = {
    parameter('code ? true) { code =>
      //check if execute only is enabled, and if there is a discrepancy between the current user's namespace
      //and that of the entity we are trying to fetch
      if (executeOnly && user.namespace.name != entityName.namespace) {
        terminate(Forbidden, forbiddenGetAction(entityName.path.asString))
      } else {
        fetchEntity(entityName, env, code)
      }
    }
  }

  /**
   * Gets all actions in a path.
   *
   * Responses are one of (Code, Message)
   * - 200 [] or [WhiskAction as JSON]
   * - 500 Internal Server Error
   */
  override def list(user: Identity, namespace: EntityPath)(implicit transid: TransactionId) = {
    parameter(
      'skip.as[ListSkip] ? ListSkip(collection.defaultListSkip),
      'limit.as[ListLimit] ? ListLimit(collection.defaultListLimit),
      'count ? false) { (skip, limit, count) =>
      if (!count) {
        listEntities {
          WhiskAction.listCollectionInNamespace(entityStore, namespace, skip.n, limit.n, includeDocs = false) map {
            list =>
              list.fold((js) => js, (as) => as.map(WhiskAction.serdes.write(_)))
          }
        }
      } else {
        countEntities {
          WhiskAction.countCollectionInNamespace(entityStore, namespace, skip.n)
        }
      }
    }
  }

  /** Replaces default namespaces in a vector of components from a sequence with appropriate namespace. */
  private def resolveDefaultNamespace(components: Vector[FullyQualifiedEntityName],
                                      user: Identity): Vector[FullyQualifiedEntityName] = {
    // if components are part of the default namespace, they contain `_`; replace it!
    val resolvedComponents = components map { c =>
      FullyQualifiedEntityName(c.path.resolveNamespace(user.namespace), c.name)
    }
    resolvedComponents
  }

  /**
   * Creates a WhiskAction instance from the PUT request.
   */
  private def makeWhiskAction(content: WhiskActionPut, entityName: FullyQualifiedEntityName)(
    implicit transid: TransactionId) = {
    val exec = content.exec.get
    val limits = content.limits map { l =>
      ActionLimits(
        l.timeout getOrElse TimeLimit(),
        l.memory getOrElse MemoryLimit(),
        l.logs getOrElse LogLimit(),
        l.concurrency getOrElse IntraConcurrencyLimit(),
        l.instances)
    } getOrElse ActionLimits()
    // This is temporary while we are making sequencing directly supported in the controller.
    // The parameter override allows this to work with Pipecode.code. Any parameters other
    // than the action sequence itself are discarded and have no effect.
    // Note: While changing the implementation of sequences, components now store the fully qualified entity names
    // (which loses the leading "/"). Adding it back while both versions of the code are in place.
    val parameters = exec match {
      case seq: SequenceExec =>
        Parameters("_actions", JsArray(seq.components map { _.qualifiedNameWithLeadingSlash.toJson }))
      case _ => content.parameters getOrElse Parameters()
    }

    WhiskAction(
      entityName.path,
      entityName.name,
      exec,
      parameters,
      limits,
      content.version getOrElse SemVer(),
      content.publish getOrElse false,
      WhiskActionsApi.amendAnnotations(content.annotations getOrElse Parameters(), exec))
  }

  /** For a sequence action, gather referenced entities and authorize access. */
  private def entitleReferencedEntities(user: Identity, right: Privilege, exec: Option[Exec])(
    implicit transid: TransactionId) = {
    exec match {
      case Some(seq: SequenceExec) =>
        logging.debug(this, "checking if sequence components are accessible")
        entitlementProvider.check(user, right, referencedEntities(seq), noThrottle = true)
      case _ => Future.successful(true)
    }
  }

  private def entitleReferencedEntitiesMetaData(user: Identity, right: Privilege, exec: Option[ExecMetaDataBase])(
    implicit transid: TransactionId) = {
    exec match {
      case Some(seq: SequenceExecMetaData) =>
        logging.info(this, "checking if sequence components are accessible")
        entitlementProvider.check(user, right, referencedEntities(seq), noThrottle = true)
      case _ => Future.successful(true)
    }
  }

  /** Creates a WhiskAction from PUT content, generating default values where necessary. */
  private def make(user: Identity, entityName: FullyQualifiedEntityName, content: WhiskActionPut)(
    implicit transid: TransactionId) = {
    checkInstanceConcurrencyLessThanNamespaceConcurrency(user, content) flatMap { _ =>
      content.exec map {
        case seq: SequenceExec =>
          // check that the sequence conforms to max length and no recursion rules
          checkSequenceActionLimits(entityName, seq.components) map { _ =>
            makeWhiskAction(content.replace(seq), entityName)
          }
        case supportedExec if !supportedExec.deprecated =>
          Future successful makeWhiskAction(content, entityName)
        case deprecatedExec =>
          Future failed RejectRequest(BadRequest, runtimeDeprecated(deprecatedExec))

      } getOrElse Future.failed(RejectRequest(BadRequest, "exec undefined"))
    }
  }

  /** Updates a WhiskAction from PUT content, merging old action where necessary. */
  private def update(user: Identity, content: WhiskActionPut)(action: WhiskAction)(implicit transid: TransactionId) = {
    checkInstanceConcurrencyLessThanNamespaceConcurrency(user, content) flatMap { _ =>
      content.exec map {
        case seq: SequenceExec =>
          // check that the sequence conforms to max length and no recursion rules
          checkSequenceActionLimits(FullyQualifiedEntityName(action.namespace, action.name), seq.components) map { _ =>
            updateWhiskAction(content.replace(seq), action)
          }
        case supportedExec if !supportedExec.deprecated =>
          Future successful updateWhiskAction(content, action)
        case deprecatedExec =>
          Future failed RejectRequest(BadRequest, runtimeDeprecated(deprecatedExec))
      } getOrElse {
        if (!action.exec.deprecated) {
          Future successful updateWhiskAction(content, action)
        } else {
          Future failed RejectRequest(BadRequest, runtimeDeprecated(action.exec))
        }
      }
    }
  }

  /**
   * Updates a WhiskAction instance from the PUT request.
   */
  private def updateWhiskAction(content: WhiskActionPut, action: WhiskAction)(implicit transid: TransactionId) = {
    val limits = content.limits map { l =>
      ActionLimits(
        l.timeout getOrElse action.limits.timeout,
        l.memory getOrElse action.limits.memory,
        l.logs getOrElse action.limits.logs,
        l.concurrency getOrElse action.limits.concurrency,
        if (l.instances.isDefined) l.instances else action.limits.instances)
    } getOrElse action.limits

    // This is temporary while we are making sequencing directly supported in the controller.
    // Actions that are updated with a sequence will have their parameter property overridden.
    // Actions that are updated with non-sequence actions will either set the parameter property according to
    // the content provided, or if that is not defined, and iff the previous version of the action was not a
    // sequence, inherit previous parameters. This is because sequence parameters are special and should not
    // leak to non-sequence actions.
    // If updating an action but not specifying a new exec type, then preserve the previous parameters if the
    // existing type of the action is a sequence (regardless of what parameters may be defined in the content)
    // otherwise, parameters are inferred from the content or previous values.
    // Note: While changing the implementation of sequences, components now store the fully qualified entity names
    // (which loses the leading "/"). Adding it back while both versions of the code are in place. This will disappear completely
    // once the version of sequences with "pipe.js" is removed.
    val parameters = content.exec map {
      case seq: SequenceExec =>
        Parameters("_actions", JsArray(seq.components map { c =>
          JsString("/" + c.toString)
        }))
      case _ =>
        content.parameters getOrElse {
          action.exec match {
            case seq: SequenceExec => Parameters()
            case _                 => action.parameters
          }
        }
    } getOrElse {
      action.exec match {
        case seq: SequenceExec => action.parameters // discard content.parameters
        case _                 => content.parameters getOrElse action.parameters
      }
    }

    val exec = content.exec getOrElse action.exec

    val newAnnotations = content.delAnnotations
      .map { annotationArray =>
        annotationArray.foldRight(action.annotations)((a: String, b: Parameters) => b - a)
      }
      .map(_ ++ content.annotations)
      .getOrElse(action.annotations ++ content.annotations)

    WhiskAction(
      action.namespace,
      action.name,
      exec,
      parameters,
      limits,
      content.version getOrElse action.version.upPatch,
      content.publish getOrElse action.publish,
      WhiskActionsApi.amendAnnotations(newAnnotations, exec, create = false))
      .revision[WhiskAction](action.docinfo.rev)
  }

  /**
   * Lists actions in package or binding. The router authorized the subject for the package
   * (if binding, then authorized subject for both the binding and the references package)
   * and iff authorized, this method is reached to lists actions.
   *
   * Note that when listing actions in a binding, the namespace on the actions will be that
   * of the referenced packaged, not the binding.
   */
  private def listPackageActions(user: Identity, pkgName: FullyQualifiedEntityName)(implicit transid: TransactionId) = {
    // get the package to determine if it is a package or reference
    // (this will set the appropriate namespace), and then list actions
    // NOTE: these fetches are redundant with those from the authorization
    // and should hit the cache to ameliorate the cost; this can be improved
    // but requires communicating back from the authorization service the
    // resolved namespace
    getEntity(WhiskPackage.get(entityStore, pkgName.toDocId), Some { (wp: WhiskPackage) =>
      val pkgns = wp.binding map { b =>
        logging.debug(this, s"list actions in package binding '${wp.name}' -> '$b'")
        b.namespace.addPath(b.name)
      } getOrElse {
        logging.debug(this, s"list actions in package '${wp.name}'")
        pkgName.path.addPath(wp.name)
      }
      // list actions in resolved namespace
      list(user, pkgns)
    })
  }

  private def checkActionLimits(user: Identity, content: WhiskActionPut)(
    implicit transid: TransactionId): Future[Unit] = {
    logging.debug(this, "checking the namespace and system limit for action")
    try {
      // check namespace limits
      content.limits foreach { limit =>
        limit.memory foreach (_.checkNamespaceLimit(user))
        limit.timeout foreach (_.checkNamespaceLimit(user))
        limit.logs foreach (_.checkNamespaceLimit(user))
        limit.concurrency foreach (_.checkNamespaceLimit(user))
      }
      Future.successful(())
    } catch {
      case e: ActionLimitsException => Future failed RejectRequest(BadRequest, e.getMessage)
    }
  }

  /**
   * Checks that the sequence is not cyclic and that the number of atomic actions in the "inlined" sequence is lower than max allowed.
   *
   * @param sequenceAction is the action sequence to check
   * @param components the components of the sequence
   */
  private def checkSequenceActionLimits(
    sequenceAction: FullyQualifiedEntityName,
    components: Vector[FullyQualifiedEntityName])(implicit transid: TransactionId): Future[Unit] = {
    // first checks that current sequence length is allowed
    // then traverses all actions in the sequence, inlining any that are sequences
    val future = if (components.size > actionSequenceLimit) {
      Future.failed(TooManyActionsInSequence())
    } else if (components.size == 0) {
      Future.failed(NoComponentInSequence())
    } else {
      // resolve the action document id (if it's in a package/binding);
      // this assumes that entityStore is the same for actions and packages
      WhiskAction.resolveAction(entityStore, sequenceAction) flatMap { resolvedSeq =>
        val atomicActionCnt = countAtomicActionsAndCheckCycle(resolvedSeq, components)
        atomicActionCnt map { count =>
          logging.debug(this, s"sequence '$sequenceAction' atomic action count $count")
          if (count > actionSequenceLimit) {
            throw TooManyActionsInSequence()
          }
        }
      }
    }

    future recoverWith {
      case _: TooManyActionsInSequence => Future failed RejectRequest(BadRequest, sequenceIsTooLong)
      case _: NoComponentInSequence    => Future failed RejectRequest(BadRequest, sequenceNoComponent)
      case _: SequenceWithCycle        => Future failed RejectRequest(BadRequest, sequenceIsCyclic)
      case _: NoDocumentException      => Future failed RejectRequest(BadRequest, sequenceComponentNotFound)
    }
  }

  private def checkInstanceConcurrencyLessThanNamespaceConcurrency(user: Identity, content: WhiskActionPut)(
    implicit transid: TransactionId): Future[Unit] = {
    val namespaceConcurrencyLimit =
      user.limits.concurrentInvocations.getOrElse(whiskConfig.actionInvokeConcurrentLimit.toInt)
    content.limits
      .map(
        l =>
          l.instances
            .map(
              m =>
                if (m.maxConcurrentInstances > namespaceConcurrencyLimit)
                  Future failed RejectRequest(
                    BadRequest,
                    maxActionInstanceConcurrencyExceedsNamespace(namespaceConcurrencyLimit))
                else Future.successful({}))
            .getOrElse(Future.successful({})))
      .getOrElse(Future.successful({}))
  }

  /**
   * Counts the number of atomic actions in a sequence and checks for potential cycles. The latter is done
   * by inlining any sequence components that are themselves sequences and checking if there if a reference to
   * the given original sequence.
   *
   * @param origSequence the original sequence that is updated/created which generated the checks
   * @param components the components of the a sequence to check if they reference the original sequence
   * @return Future with the number of atomic actions in the current sequence or an appropriate error if there is a cycle or a non-existent action reference
   */
  private def countAtomicActionsAndCheckCycle(
    origSequence: FullyQualifiedEntityName,
    components: Vector[FullyQualifiedEntityName])(implicit transid: TransactionId): Future[Int] = {
    if (components.size > actionSequenceLimit) {
      Future.failed(TooManyActionsInSequence())
    } else {
      // resolve components wrt any package bindings
      val resolvedComponentsFutures = components map { c =>
        WhiskAction.resolveAction(entityStore, c)
      }
      // traverse the sequence structure by checking each of its components and do the following:
      // 1. check whether any action (sequence or not) referred by the sequence (directly or indirectly)
      //    is the same as the original sequence (aka origSequence)
      // 2. count the atomic actions each component has (by "inlining" all sequences)
      val actionCountsFutures = resolvedComponentsFutures map {
        _ flatMap { resolvedComponent =>
          // check whether this component is the same as origSequence
          // this can happen when updating an atomic action to become a sequence
          if (origSequence == resolvedComponent) {
            Future failed SequenceWithCycle()
          } else {
            // check whether component is a sequence or an atomic action
            // if the component does not exist, the future will fail with appropriate error
            WhiskAction.get(entityStore, resolvedComponent.toDocId) flatMap { wskComponent =>
              wskComponent.exec match {
                case SequenceExec(seqComponents) =>
                  // sequence action, count the number of atomic actions in this sequence
                  countAtomicActionsAndCheckCycle(origSequence, seqComponents)
                case _ => Future successful 1 // atomic action count is one
              }
            }
          }
        }
      }
      // collapse the futures in one future
      val actionCountsFuture = Future.sequence(actionCountsFutures)
      // sum up all individual action counts per component
      val totalActionCount = actionCountsFuture map { actionCounts =>
        actionCounts.foldLeft(0)(_ + _)
      }
      totalActionCount
    }
  }

  /** Max atomic action count allowed for sequences. */
  private lazy val actionSequenceLimit = whiskConfig.actionSequenceLimit.toInt

  implicit val stringToFiniteDuration: Unmarshaller[String, FiniteDuration] = {
    Unmarshaller.strict[String, FiniteDuration] { value =>
      val max = controllerActivationConfig.maxWaitForBlockingActivation.toMillis

      Try { value.toInt } match {
        case Success(i) if i > 0 && i <= max => i.milliseconds
        case _ =>
          throw new IllegalArgumentException(
            Messages.invalidTimeout(controllerActivationConfig.maxWaitForBlockingActivation))
      }
    }
  }

  /** Custom unmarshaller for query parameters "limit" for "list" operations. */
  private implicit val stringToListLimit: Unmarshaller[String, ListLimit] = RestApiCommons.stringToListLimit(collection)

  /** Custom unmarshaller for query parameters "skip" for "list" operations. */
  private implicit val stringToListSkip: Unmarshaller[String, ListSkip] = RestApiCommons.stringToListSkip(collection)

}

private case class TooManyActionsInSequence() extends IllegalArgumentException
private case class NoComponentInSequence() extends IllegalArgumentException
private case class SequenceWithCycle() extends IllegalArgumentException
