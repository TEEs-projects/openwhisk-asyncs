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

package org.apache.openwhisk.core.containerpool.v2

import java.lang.management.ManagementFactory
import java.net.InetSocketAddress
import java.time.Instant
import java.util.Base64
import java.util.concurrent.ConcurrentHashMap
import org.apache.pekko.actor.Status.{Failure => FailureMessage}
import org.apache.pekko.actor.{ActorRef, ActorRefFactory, ActorSystem, FSM, Props, Stash}
import org.apache.pekko.event.Logging.InfoLevel
import org.apache.pekko.io.{IO, Tcp}
import org.apache.pekko.pattern.pipe
import org.apache.openwhisk.common.tracing.WhiskTracerProvider
import org.apache.openwhisk.common.{LoggingMarkers, TransactionId, _}
import org.apache.openwhisk.core.ConfigKeys
import org.apache.openwhisk.core.ack.ActiveAck
import org.apache.openwhisk.core.connector.{
  ActivationMessage,
  CombinedCompletionAndResultMessage,
  CompletionMessage,
  ResultMessage
}
import org.apache.openwhisk.core.containerpool._
import org.apache.openwhisk.core.containerpool.logging.LogCollectingException
import org.apache.openwhisk.core.containerpool.v2.FunctionPullingContainerProxy.{
  constructWhiskActivation,
  containerName
}
import org.apache.openwhisk.core.database._
import org.apache.openwhisk.core.entity.ExecManifest.ImageName
import org.apache.openwhisk.core.entity.size._
import org.apache.openwhisk.core.entity.{ExecutableWhiskAction, ActivationResponse => ExecutionResponse, _}
import org.apache.openwhisk.core.etcd.EtcdKV.ContainerKeys
import org.apache.openwhisk.core.invoker.Invoker.LogsCollector
import org.apache.openwhisk.core.invoker.NamespaceBlacklist
import org.apache.openwhisk.core.scheduler.SchedulerEndpoints
import org.apache.openwhisk.core.scheduler.queue.{
  ProtectedEnvelopeDirection,
  ProtectedEnvelopeV1,
  ProtectedObjectKind,
  TargetBoundActivationContent
}
import org.apache.openwhisk.core.service.{RegisterData, UnregisterData}
import org.apache.openwhisk.grpc.RescheduleResponse
import org.apache.openwhisk.http.Messages
import pureconfig.loadConfigOrThrow
import spray.json.DefaultJsonProtocol.{StringJsonFormat, _}
import spray.json._
import pureconfig.generic.auto._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

// Events used internally
case class RunActivation(action: ExecutableWhiskAction, msg: ActivationMessage)
case class RunActivationCompleted(container: Container, action: ExecutableWhiskAction, duration: Option[Long])
case class InitCodeCompleted(data: WarmData)
private[v2] case class PreWarmContainerReady(data: PreWarmData, binding: Option[(TargetContainer, TargetBinding)])
private[v2] case class ColdContainerReady(job: Initialize,
                                          container: Container,
                                          binding: Option[(TargetContainer, TargetBinding)])
private[v2] case object TargetBoundContainerRemovalCompleted
private[v2] case class TargetBoundContainerRemovalFailed(cause: Throwable)

// Events received by the actor
case class Initialize(invocationNamespace: String,
                      fqn: FullyQualifiedEntityName,
                      action: ExecutableWhiskAction,
                      schedulerHost: String,
                      rpcPort: Int,
                      transId: TransactionId)
case class Start(exec: CodeExec[_], memoryLimit: ByteSize, ttl: Option[FiniteDuration] = None)

// Event sent by the actor
case class ContainerCreationFailed(throwable: Throwable)
case class ContainerIsPaused(data: WarmData)
case class ClientCreationFailed(throwable: Throwable,
                                container: Container,
                                invocationNamespace: String,
                                action: ExecutableWhiskAction)
case class ReadyToWork(data: PreWarmData)
case class Initialized(data: InitializedData)
case class Resumed(data: WarmData)
case class ResumeFailed(data: WarmData)
case class RecreateClient(action: ExecutableWhiskAction)
case object PingCache
case class DetermineKeepContainer(attempt: Int)

// States
sealed trait ProxyState
case object LeaseStart extends ProxyState
case object Uninitialized extends ProxyState
case object CreatingContainer extends ProxyState
case object ContainerCreated extends ProxyState
case object CreatingClient extends ProxyState
case object ClientCreated extends ProxyState
case object Running extends ProxyState
case object Pausing extends ProxyState
case object Paused extends ProxyState
case object Removing extends ProxyState
case object Rescheduling extends ProxyState

// Errors
case class ContainerHealthErrorWithResumedRun(tid: TransactionId, msg: String, resumeRun: RunActivation)
    extends Exception(msg)

// Data
sealed abstract class Data(val memoryLimit: ByteSize) {
  def getContainer: Option[Container]
}
case class NonexistentData() extends Data(0.B) {
  override def getContainer = None
}
case class MemoryData(override val memoryLimit: ByteSize) extends Data(memoryLimit) {
  override def getContainer = None
}
trait WithClient { val clientProxy: ActorRef }
case class PreWarmData(container: Container,
                       kind: String,
                       override val memoryLimit: ByteSize,
                       expires: Option[Deadline] = None)
    extends Data(memoryLimit) {
  override def getContainer = Some(container)
  def isExpired(): Boolean = expires.exists(_.isOverdue())
}

object BasicContainerInfo extends DefaultJsonProtocol {
  implicit val prewarmedPoolSerdes = jsonFormat4(BasicContainerInfo.apply)
}

sealed case class BasicContainerInfo(containerId: String, namespace: String, action: String, kind: String)

sealed abstract class ContainerAvailableData(container: Container,
                                             invocationNamespace: String,
                                             action: ExecutableWhiskAction)
    extends Data(action.limits.memory.megabytes.MB) {
  override def getContainer = Some(container)

  val basicContainerInfo =
    BasicContainerInfo(container.containerId.asString, invocationNamespace, action.name.asString, action.exec.kind)
}

case class ContainerCreatedData(container: Container, invocationNamespace: String, action: ExecutableWhiskAction)
    extends ContainerAvailableData(container, invocationNamespace, action)

case class InitializedData(container: Container,
                           invocationNamespace: String,
                           action: ExecutableWhiskAction,
                           override val clientProxy: ActorRef)
    extends ContainerAvailableData(container, invocationNamespace, action)
    with WithClient {
  override def getContainer = Some(container)
  def toReschedulingData(resumeRun: RunActivation) =
    ReschedulingData(container, invocationNamespace, action, clientProxy, resumeRun)
}

case class WarmData(container: Container,
                    invocationNamespace: String,
                    action: ExecutableWhiskAction,
                    revision: DocRevision,
                    lastUsed: Instant,
                    override val clientProxy: ActorRef)
    extends ContainerAvailableData(container, invocationNamespace, action)
    with WithClient {
  override def getContainer = Some(container)
  def toReschedulingData(resumeRun: RunActivation) =
    ReschedulingData(container, invocationNamespace, action, clientProxy, resumeRun)
}

case class ReschedulingData(container: Container,
                            invocationNamespace: String,
                            action: ExecutableWhiskAction,
                            clientProxy: ActorRef,
                            resumeRun: RunActivation)
    extends ContainerAvailableData(container, invocationNamespace, action)
    with WithClient

class FunctionPullingContainerProxy(
  factory: (TransactionId,
            String,
            ImageName,
            Boolean,
            ByteSize,
            Int,
            Option[Double],
            Option[ExecutableWhiskAction]) => Future[Container],
  entityStore: ArtifactStore[WhiskEntity],
  namespaceBlacklist: NamespaceBlacklist,
  get: (ArtifactStore[WhiskEntity], DocId, DocRevision, Boolean, Boolean) => Future[WhiskAction],
  dataManagementService: ActorRef,
  clientProxyFactory: (ActorRefFactory,
                       String,
                       FullyQualifiedEntityName,
                       DocRevision,
                       String,
                       Int,
                       ContainerId,
                       Option[Long]) => ActorRef,
  sendActiveAck: ActiveAck,
  storeActivation: (TransactionId, WhiskActivation, Boolean, UserContext) => Future[Any],
  collectLogs: LogsCollector,
  getLiveContainerCount: (String, FullyQualifiedEntityName, DocRevision) => Future[Long],
  getWarmedContainerLimit: (String) => Future[(Int, FiniteDuration)],
  instance: InvokerInstanceId,
  invokerHealthManager: ActorRef,
  poolConfig: ContainerPoolConfig,
  timeoutConfig: ContainerProxyTimeoutConfig,
  healtCheckConfig: ContainerProxyHealthCheckConfig,
  testTcp: Option[ActorRef],
  targetBindingProvider: TargetBindingProvider = TargetBindingProvider.Disabled)(implicit actorSystem: ActorSystem,
                                                                                 logging: Logging)
    extends FSM[ProxyState, Data]
    with Stash {
  startWith(Uninitialized, NonexistentData())

  implicit val ec = actorSystem.dispatcher

  private val UnusedTimeoutName = "UnusedTimeout"
  private val unusedTimeout = timeoutConfig.pauseGrace
  private val IdleTimeoutName = "PausingTimeout"
  private val idleTimeout = timeoutConfig.idleContainer
  private val KeepingTimeoutName = "KeepingTimeout"
  private val RunningActivationTimeoutName = "RunningActivationTimeout"
  private val runningActivationTimeout = 10.seconds
  private val PingCacheName = "PingCache"
  private val pingCacheInterval = 1.minute
  private var timedOut = false
  private var activeTargetBinding = Option.empty[(TargetContainer, TargetBinding)]
  private var targetBoundRemovalStarted = false
  private var pendingTargetBoundRemoval = Option.empty[(Container, Boolean)]
  private val c1TimingProcessId = ManagementFactory.getRuntimeMXBean.getName.takeWhile(_ != '@')
  private val c1TimingNode = sys.env.get("C1_TIMING_NODE_ID").orElse(sys.env.get("HOSTNAME")).getOrElse("")
  private val c1InternalRescheduleInjectionReason = "c1_internal_reschedule_injection"

  var healthPingActor: Option[ActorRef] = None //setup after prewarm starts
  val tcp: ActorRef = testTcp.getOrElse(IO(Tcp)) //allows to testing interaction with Tcp extension

  val runningActivations = new ConcurrentHashMap[String, Boolean]
  private val c1InternalRescheduleInjectionEnabled = sys.env
    .get("C1_INTERNAL_RESCHEDULE_INJECTION_ENABLED")
    .map(_.trim.toLowerCase)
    .exists(value => value == "1" || value == "true" || value == "yes")
  private val reusableConcurrencySkipActivationStore =
    FunctionPullingContainerProxy.reusableConcurrencyStoreSkipEnabled(sys.env)

  private def c1TimingSanitize(value: String): String =
    value.replace('|', '_').replace('\n', ' ').replace('\r', ' ')

  private def c1TimingField(key: String, value: String): String = s"$key=${c1TimingSanitize(value)}"

  private def isBackendPressureActivation(msg: ActivationMessage): Boolean =
    msg.metrics.get("c1_backend_pressure").contains(1L)

  private def shouldSkipBackendPressureActivationStore(msg: ActivationMessage): Boolean =
    sys.env.get("C1_BACKEND_PRESSURE_SKIP_ACTIVATION_STORE").contains("1") && isBackendPressureActivation(msg)

  private def guardedStoreActivation(tid: TransactionId,
                                     activation: WhiskActivation,
                                     msg: ActivationMessage,
                                     actionKind: String,
                                     isBlocking: Boolean,
                                     context: UserContext): Future[Any] = {
    if (shouldSkipBackendPressureActivationStore(msg)) {
      val fields = Seq(
        c1TimingField("activation_id", msg.activationId.asString),
        c1TimingField("tid", tid.id),
        c1TimingField("namespace", msg.user.namespace.name.asString),
        c1TimingField("blocking", isBlocking.toString),
        c1TimingField("status_code", activation.response.statusCode.toString),
        c1TimingField("reason", "backend_pressure_store_skip"))
      logging.info(this, s"C1_BACKEND_PRESSURE_SKIP_STORE|${fields.mkString("|")}")(tid)
      Future.successful(())
    } else {
      val skipReason = FunctionPullingContainerProxy.reusableConcurrencyStoreSkipReason(
        reusableConcurrencySkipActivationStore,
        actionKind)
      skipReason.foreach { reason =>
        val fields = Seq(
          c1TimingField("activation_id", msg.activationId.asString),
          c1TimingField("tid", tid.id),
          c1TimingField("namespace", msg.user.namespace.name.asString),
          c1TimingField("blocking", isBlocking.toString),
          c1TimingField("status_code", activation.response.statusCode.toString),
          c1TimingField("profile", "reusable-concurrency"),
          c1TimingField("action_kind", actionKind),
          c1TimingField("store_status", "skipped"),
          c1TimingField("store_path", "not_on_path"),
          c1TimingField("reason", reason))
        logging.info(this, s"REUSABLE_CONCURRENCY_SKIP_STORE|${fields.mkString("|")}")(tid)
      }
      FunctionPullingContainerProxy.storeActivationUnlessSkipped(skipReason) {
        storeActivation(tid, activation, isBlocking, context)
      }
    }
  }

  private def c1BackendPressureResultField(fields: Map[String, JsValue], key: String): Option[String] =
    fields.get(key).collect {
      case JsString(value)  => value
      case JsNumber(value)  => value.toString
      case JsBoolean(value) => value.toString
    }

  private def emitC1BackendPressureWorkloadTiming(msg: ActivationMessage, activation: WhiskActivation): Unit = {
    if (shouldSkipBackendPressureActivationStore(msg)) {
      activation.response.result match {
        case Some(JsObject(fields)) =>
          val resultFields = fields.get(ExecutionResponse.ERROR_FIELD) match {
            case Some(JsObject(errorFields)) => errorFields
            case _                           => fields
          }
          val runId = c1BackendPressureResultField(resultFields, "run_id")
          val logicalRequestId = c1BackendPressureResultField(resultFields, "logical_request_id")
          val workloadDurationNs = c1BackendPressureResultField(resultFields, "workload_duration_ns")
          if (runId.isDefined && logicalRequestId.isDefined && workloadDurationNs.isDefined) {
            val markerFields = Seq(
              c1TimingField("run_id", runId.get),
              c1TimingField("logical_request_id", logicalRequestId.get),
              c1TimingField("activation_id", msg.activationId.asString),
              c1TimingField("workload_id", c1BackendPressureResultField(resultFields, "workload_id").getOrElse("")),
              c1TimingField("workload_kind", c1BackendPressureResultField(resultFields, "workload_kind").getOrElse("")),
              c1TimingField("workload_duration_ns", workloadDurationNs.get),
              c1TimingField(
                "compress_mode",
                c1BackendPressureResultField(resultFields, "workload_compress_mode").getOrElse("")),
              c1TimingField(
                "compress_bytes",
                c1BackendPressureResultField(resultFields, "workload_compress_bytes").getOrElse("")),
              c1TimingField(
                "compress_level",
                c1BackendPressureResultField(resultFields, "workload_compress_level").getOrElse("")),
              c1TimingField(
                "output_bytes",
                c1BackendPressureResultField(resultFields, "workload_output_bytes").getOrElse("")),
              c1TimingField(
                "failure_probability",
                c1BackendPressureResultField(resultFields, "failure_probability").getOrElse("")),
              c1TimingField(
                "scheduled_failure",
                c1BackendPressureResultField(resultFields, "scheduled_failure").getOrElse("")),
              c1TimingField(
                "failure_reason",
                c1BackendPressureResultField(resultFields, "failure_reason").getOrElse("")))
            logging.info(this, s"C1_BACKEND_PRESSURE_WORKLOAD_TIMING|${markerFields.mkString("|")}")(msg.transid)
          }
        case _ =>
      }
    }
  }

  private def emitC1BackendPressureAsynCSEvidence(msg: ActivationMessage, activation: WhiskActivation): Unit =
    FunctionPullingContainerProxy
      .c1BackendPressureAsynCSEvidenceLines(
        isBackendPressureActivation(msg),
        activation.response.result,
        msg.activationId.asString,
        c1TimingNode,
        msg.transid.id)
      .foreach(line => logging.info(this, line)(msg.transid))

  private def emitC1TimingEvent(eventCode: String, boundaryName: String, msg: ActivationMessage): Unit = {
    val unixNs = System.currentTimeMillis() * 1000000L
    val monoNs = System.nanoTime()
    val fields = Seq(
      c1TimingField("event_code", eventCode),
      c1TimingField("activation_id", msg.activationId.asString),
      c1TimingField("boundary_name", boundaryName),
      c1TimingField("node", c1TimingNode),
      c1TimingField("process", "openwhisk_invoker"),
      c1TimingField("pid", c1TimingProcessId),
      c1TimingField("tid", msg.transid.id),
      c1TimingField("unix_ns", unixNs.toString),
      c1TimingField("mono_ns", monoNs.toString),
      c1TimingField("clock_domain", "openwhisk_invoker_jvm_mono"))
    logging.info(this, s"C1TIMING_EVENT|${fields.mkString("|")}")(msg.transid)
  }

  private def c1InternalRescheduleRequested(parameters: JsValue): Boolean =
    parameters match {
      case jsObject: JsObject => jsObject.fields.get("c1_internal_reschedule_requested").contains(JsBoolean(true))
      case _                  => false
    }

  private def c1ConsumeInternalRescheduleRequest(msg: ActivationMessage): ActivationMessage =
    msg.content match {
      case Some(jsObject: JsObject) =>
        val consumedFields =
          (jsObject.fields - "c1_internal_reschedule_requested") +
            ("c1_internal_reschedule_injected" -> JsBoolean(true))
        msg.copy(content = Some(JsObject(consumedFields)))
      case _ => msg
    }

  private def awaitTargetBinding(container: Container,
                                 kind: String): Future[Option[(TargetContainer, TargetBinding)]] = {
    if (targetBindingProvider.requiresBinding(kind)) {
      val target = TargetContainer(
        container.containerId,
        container.addr,
        kind,
        (bindingId, timeout) => container.activateTargetBinding(bindingId, timeout)(TransactionId.invokerNanny))
      targetBindingProvider.awaitReady(target).map(binding => Some(target -> binding))
    } else {
      Future.successful(None)
    }
  }

  private def awaitTargetBindingOrDestroy(container: Container,
                                          kind: String): Future[Option[(TargetContainer, TargetBinding)]] =
    awaitTargetBinding(container, kind).recoverWith {
      case t =>
        logging.error(this, s"target binding readiness failed for ${container.containerId.asString}: ${t.getMessage}")
        destroyContainer(container).transformWith(_ => Future.failed(t))
    }

  private def createActivationClient(job: Initialize,
                                     container: Container): Either[ClientCreationFailed, InitializedData] =
    Try(
      clientProxyFactory(
        context,
        job.invocationNamespace,
        job.fqn,
        job.action.rev,
        job.schedulerHost,
        job.rpcPort,
        container.containerId,
        activeTargetBinding.map(_._2.id))) match {
      case Success(clientProxy) =>
        Right(InitializedData(container, job.invocationNamespace, job.action, clientProxy))
      case Failure(t) =>
        logging.error(this, s"failed to create activation client for ${job.action} caused by: $t")
        Left(ClientCreationFailed(t, container, job.invocationNamespace, job.action))
    }

  when(Uninitialized) {
    // pre warm a container (creates a stem cell container)
    case Event(job: Start, _) =>
      factory(
        TransactionId.invokerWarmup,
        containerName(instance, "prewarm", job.exec.kind),
        job.exec.image,
        job.exec.pull,
        job.memoryLimit,
        poolConfig.cpuShare(job.memoryLimit),
        poolConfig.cpuLimit(job.memoryLimit),
        None)
        .flatMap { container =>
          awaitTargetBindingOrDestroy(container, job.exec.kind).map { binding =>
            PreWarmContainerReady(
              PreWarmData(container, job.exec.kind, job.memoryLimit, expires = job.ttl.map(_.fromNow)),
              binding)
          }
        }
        .pipeTo(self)
      goto(CreatingContainer)

    // cold start
    case Event(job: Initialize, _) =>
      factory( // create a new container
        TransactionId.invokerColdstart,
        containerName(instance, job.action.namespace.namespace, job.action.name.asString),
        job.action.exec.image,
        job.action.exec.pull,
        job.action.limits.memory.megabytes.MB,
        poolConfig.cpuShare(job.action.limits.memory.megabytes.MB),
        poolConfig.cpuLimit(job.action.limits.memory.megabytes.MB),
        None)
        .andThen {
          case Failure(t) =>
            context.parent ! ContainerCreationFailed(t)
        }
        .flatMap { container =>
          logging.debug(this, s"a container ${container.containerId} is created for ${job.action}")
          awaitTargetBindingOrDestroy(container, job.action.exec.kind).map { binding =>
            ColdContainerReady(job, container, binding)
          }
        }
        .pipeTo(self)

      goto(CreatingClient)

    case _ => delay
  }

  when(CreatingContainer) {
    // container was successfully obtained
    case Event(completed: PreWarmContainerReady, _: NonexistentData) =>
      activeTargetBinding = completed.binding
      context.parent ! ReadyToWork(completed.data)
      goto(ContainerCreated) using completed.data

    // container creation failed
    case Event(t: FailureMessage, _: NonexistentData) =>
      context.parent ! ContainerRemoved(true)
      stop()

    case _ => delay
  }

  // prewarmed state, container created
  when(ContainerCreated) {
    case Event(job: Initialize, data: PreWarmData) =>
      createActivationClient(job, data.container).fold(self ! _, self ! _)

      goto(CreatingClient)

    case Event(Remove, data: PreWarmData) =>
      cleanUp(data.container, None, false)

    // prewarm container failed by health check
    case Event(_: FailureMessage, data: PreWarmData) =>
      MetricEmitter.emitCounterMetric(LoggingMarkers.INVOKER_CONTAINER_HEALTH_FAILED_PREWARM)
      cleanUp(data.container, None)

    case _ => delay
  }

  when(CreatingClient) {
    case Event(ready: ColdContainerReady, _) =>
      activeTargetBinding = ready.binding
      createActivationClient(ready.job, ready.container).fold(self ! _, self ! _)
      stay()

    // wait for client creation when cold start
    case Event(job: InitializedData, _) =>
      job.clientProxy ! StartClient

      stay() using job

    // client was successfully obtained
    case Event(ClientCreationCompleted, data: InitializedData) =>
      val fqn = data.action.fullyQualifiedName(true)
      val revision = data.action.rev
      dataManagementService ! RegisterData(
        s"${ContainerKeys.existingContainers(data.invocationNamespace, fqn, revision, Some(instance), Some(data.container.containerId))}",
        "")
      self ! data
      goto(ClientCreated)

    // client creation failed
    case Event(t: ClientCreationFailed, _) =>
      invokerHealthManager ! HealthMessage(state = false)
      cleanUp(t.container, t.invocationNamespace, t.action.fullyQualifiedName(withVersion = true), t.action.rev, None)

    case Event(ClientClosed, data: InitializedData) =>
      invokerHealthManager ! HealthMessage(state = false)
      cleanUp(
        data.container,
        data.invocationNamespace,
        data.action.fullyQualifiedName(withVersion = true),
        data.action.rev,
        None)

    // container creation failed when cold start
    case Event(_: FailureMessage, _) =>
      context.parent ! ContainerRemoved(true)
      stop()

    case _ => delay
  }

  // this is for first invocation, once the first invocation is over we are ready to trigger getActivation for action concurrency
  when(ClientCreated) {
    // 1. request activation message to client
    case Event(initializedData: InitializedData, _) =>
      context.parent ! Initialized(initializedData)
      initializedData.clientProxy ! RequestActivation()
      startTimerWithFixedDelay(PingCacheName, PingCache, pingCacheInterval)
      startSingleTimer(UnusedTimeoutName, StateTimeout, unusedTimeout)
      stay() using initializedData

    // 2. read executable action data from db
    case Event(job: ActivationMessage, data: InitializedData) =>
      timedOut = false
      cancelTimer(UnusedTimeoutName)
      handleActivationMessage(job, data.action)
        .pipeTo(self)
      stay() using data

    // 3. request initialize and run command to container
    case Event(job: RunActivation, data: InitializedData) =>
      implicit val transid = job.msg.transid
      logging.debug(this, s"received RunActivation ${job.msg.activationId} for ${job.action} in $stateName")

      initializeAndRunActivation(data.container, data.clientProxy, job.action, job.msg, Some(job))
        .map { activation =>
          RunActivationCompleted(data.container, job.action, activation.duration)
        }
        .pipeTo(self)

      // when it receives InitCodeCompleted, it will move to Running
      stay using data

    case Event(RetryRequestActivation, data: InitializedData) =>
      // if this Container is marked with time out, do not retry
      if (timedOut)
        cleanUp(
          data.container,
          data.invocationNamespace,
          data.action.fullyQualifiedName(withVersion = true),
          data.action.rev,
          Some(data.clientProxy))
      else {
        data.clientProxy ! RequestActivation()
        stay()
      }

    // code initialization was successful
    case Event(completed: InitCodeCompleted, data: InitializedData) =>
      // TODO support concurrency?
      data.clientProxy ! ContainerWarmed // this container is warmed
      1 until completed.data.action.limits.concurrency.maxConcurrent foreach { _ =>
        data.clientProxy ! RequestActivation()
      }

      goto(Running) using completed.data // set warm data

    // ContainerHealthError should cause
    case Event(FailureMessage(e: ContainerHealthErrorWithResumedRun), data: InitializedData) =>
      logging.error(
        this,
        s"container ${data.container.containerId.asString} health check failed on $stateName, ${e.resumeRun.msg.activationId} activation will be rescheduled")
      MetricEmitter.emitCounterMetric(LoggingMarkers.INVOKER_CONTAINER_HEALTH_FAILED_WARM)

      // reschedule message
      data.clientProxy ! RescheduleActivation(
        data.invocationNamespace,
        data.action.fullyQualifiedName(withVersion = true),
        data.action.rev,
        e.resumeRun.msg)

      goto(Rescheduling) using data.toReschedulingData(e.resumeRun)

    // Failed to get activation or execute the action
    case Event(t: FailureMessage, data: InitializedData) =>
      logging.error(
        this,
        s"failed to initialize a container or run an activation for ${data.action} in state: $stateName caused by: $t")
      // Stop containerProxy and ActivationClientProxy both immediately
      cleanUp(
        data.container,
        data.invocationNamespace,
        data.action.fullyQualifiedName(withVersion = true),
        data.action.rev,
        Some(data.clientProxy))

    case Event(StateTimeout, data: InitializedData) =>
      logging.info(this, s"No more activation is coming in state: $stateName, action: ${data.action}")
      // Just mark the ContainerProxy is timedout
      timedOut = true

      stay() // stay here because the ActivationClientProxy may send a new Activation message

    case Event(ClientClosed, data: InitializedData) =>
      logging.error(this, s"The Client closed in state: $stateName, action: ${data.action}")
      // Stop ContainerProxy(ActivationClientProxy will stop also when send ClientClosed to ContainerProxy).
      cleanUp(
        data.container,
        data.invocationNamespace,
        data.action.fullyQualifiedName(withVersion = true),
        data.action.rev,
        None)

    case x: Event if x.event != PingCache => delay
  }

  when(Rescheduling, stateTimeout = 10.seconds) {

    case Event(res: RescheduleResponse, data: ReschedulingData) =>
      implicit val transId = data.resumeRun.msg.transid
      if (!res.isRescheduled) {
        logging.warn(this, s"failed to reschedule the message ${data.resumeRun.msg.activationId}, clean up data")
        fallbackActivationForReschedulingData(data)
      } else {
        logging.warn(this, s"unhandled message is rescheduled, clean up data")
      }
      cleanUp(
        data.container,
        data.invocationNamespace,
        data.action.fullyQualifiedName(withVersion = true),
        data.action.rev,
        Some(data.clientProxy))

    case Event(StateTimeout, data: ReschedulingData) =>
      logging.error(this, s"Timeout for rescheduling message ${data.resumeRun.msg.activationId}, clean up data")(
        data.resumeRun.msg.transid)

      fallbackActivationForReschedulingData(data)
      cleanUp(
        data.container,
        data.invocationNamespace,
        data.action.fullyQualifiedName(withVersion = true),
        data.action.rev,
        Some(data.clientProxy))

    case x: Event if x.event != PingCache => delay
  }

  when(Running) {
    // Run was successful.
    // 1. request activation message to client
    case Event(activationResult: RunActivationCompleted, data: WarmData) =>
      // create timeout
      startSingleTimer(UnusedTimeoutName, StateTimeout, unusedTimeout)
      data.clientProxy ! RequestActivation(activationResult.duration)
      stay() using data

    // 2. read executable action data from db
    case Event(job: ActivationMessage, data: WarmData) =>
      timedOut = false
      cancelTimer(UnusedTimeoutName)
      handleActivationMessage(job, data.action)
        .pipeTo(self)
      stay() using data

    // 3. request run command to container
    case Event(job: RunActivation, data: WarmData) =>
      logging.debug(this, s"received RunActivation ${job.msg.activationId} for ${job.action} in $stateName")
      implicit val transid = job.msg.transid

      initializeAndRunActivation(data.container, data.clientProxy, job.action, job.msg, Some(job))
        .map { activation =>
          RunActivationCompleted(data.container, job.action, activation.duration)
        }
        .pipeTo(self)
      stay using data.copy(lastUsed = Instant.now)

    case Event(RetryRequestActivation, data: WarmData) =>
      // if this Container is marked with time out, do not retry
      if (timedOut) {
        data.container.suspend()(TransactionId.invokerNanny).map(_ => ContainerPaused).pipeTo(self)
        goto(Pausing)
      } else {
        data.clientProxy ! RequestActivation()
        stay()
      }

    case Event(_: ResumeFailed, data: WarmData) =>
      invokerHealthManager ! HealthMessage(state = false)
      cleanUp(
        data.container,
        data.invocationNamespace,
        data.action.fullyQualifiedName(withVersion = true),
        data.action.rev,
        Some(data.clientProxy))

    // ContainerHealthError should cause
    case Event(FailureMessage(e: ContainerHealthError), data: WarmData) =>
      logging.error(this, s"health check failed on $stateName caused by: ContainerHealthError $e")
      MetricEmitter.emitCounterMetric(LoggingMarkers.INVOKER_CONTAINER_HEALTH_FAILED_WARM)
      // Stop containerProxy and ActivationClientProxy both immediately,
      invokerHealthManager ! HealthMessage(state = false)
      cleanUp(
        data.container,
        data.invocationNamespace,
        data.action.fullyQualifiedName(withVersion = true),
        data.action.rev,
        Some(data.clientProxy))

    // ContainerHealthError should cause
    case Event(FailureMessage(e: ContainerHealthErrorWithResumedRun), data: WarmData) =>
      logging.error(
        this,
        s"container ${data.container.containerId.asString} health check failed on $stateName, ${e.resumeRun.msg.activationId} activation will be rescheduled")
      MetricEmitter.emitCounterMetric(LoggingMarkers.INVOKER_CONTAINER_HEALTH_FAILED_WARM)

      // reschedule message
      data.clientProxy ! RescheduleActivation(
        data.invocationNamespace,
        data.action.fullyQualifiedName(withVersion = true),
        data.action.rev,
        e.resumeRun.msg)

      goto(Rescheduling) using data.toReschedulingData(e.resumeRun)

    // Failed to get activation or execute the action
    case Event(t: FailureMessage, data: WarmData) =>
      logging.error(this, s"failed to init or run in state: $stateName caused by: $t")
      // Stop containerProxy and ActivationClientProxy both immediately,
      // and don't send unhealthy state message to the health manager, it's already sent.
      cleanUp(
        data.container,
        data.invocationNamespace,
        data.action.fullyQualifiedName(withVersion = true),
        data.action.rev,
        Some(data.clientProxy))

    case Event(StateTimeout, data: WarmData) =>
      logging.info(
        this,
        s"No more run activation is coming in state: $stateName, action: ${data.action}, container: ${data.container.containerId}")
      // Just mark the ContainerProxy is timedout
      timedOut = true

      stay() // stay here because the ActivationClientProxy may send a new Activation message

    case Event(ClientClosed, data: WarmData) =>
      if (runningActivations.isEmpty) {
        logging.info(this, s"The Client closed in state: $stateName, action: ${data.action}")
        // Stop ContainerProxy(ActivationClientProxy will stop also when send ClientClosed to ContainerProxy).
        cleanUp(
          data.container,
          data.invocationNamespace,
          data.action.fullyQualifiedName(withVersion = true),
          data.action.rev,
          None)
      } else {
        logging.info(
          this,
          s"Remain running activations ${runningActivations.keySet().toString()} when received ClientClosed")
        startSingleTimer(RunningActivationTimeoutName, ClientClosed, runningActivationTimeout)
        stay
      }

    // shutdown the client first and wait for any remaining activation to be executed
    // ContainerProxy will be terminated by StateTimeout if there is no further activation
    case Event(GracefulShutdown, data: WarmData) =>
      logging.info(this, s"receive GracefulShutdown for action: ${data.action}")
      // clean up the etcd data first so that the scheduler can provision more containers in advance.
      dataManagementService ! UnregisterData(
        ContainerKeys.existingContainers(
          data.invocationNamespace,
          data.action.fullyQualifiedName(true),
          data.action.rev,
          Some(instance),
          Some(data.container.containerId)))

      // Just send GracefulShutdown to ActivationClientProxy, make ActivationClientProxy throw ClientClosedException when fetchActivation next time.
      data.clientProxy ! GracefulShutdown
      stay

    case x: Event if x.event != PingCache => delay
  }

  when(Pausing) {
    case Event(ContainerPaused, data: WarmData) =>
      dataManagementService ! RegisterData(
        ContainerKeys.warmedContainers(
          data.invocationNamespace,
          data.action.fullyQualifiedName(false),
          data.revision,
          instance,
          data.container.containerId),
        "")
      // remove existing key so MemoryQueue can be terminated when timeout
      dataManagementService ! UnregisterData(
        s"${ContainerKeys.existingContainers(data.invocationNamespace, data.action.fullyQualifiedName(true), data.action.rev, Some(instance), Some(data.container.containerId))}")
      context.parent ! ContainerIsPaused(data)
      goto(Paused)

    case Event(_: FailureMessage, data: WarmData) =>
      cleanUp(
        data.container,
        data.invocationNamespace,
        data.action.fullyQualifiedName(false),
        data.action.rev,
        Some(data.clientProxy))

    case x: Event if x.event != PingCache => delay
  }

  when(Paused) {
    case Event(job: Initialize, data: WarmData) =>
      implicit val transId = job.transId
      val parent = context.parent
      cancelTimer(IdleTimeoutName)
      cancelTimer(KeepingTimeoutName)
      cancelTimer(DetermineKeepContainer.toString)
      data.container
        .resume()
        .map { _ =>
          logging.info(this, s"Resumed container ${data.container.containerId}")
          // put existing key again
          dataManagementService ! RegisterData(
            s"${ContainerKeys.existingContainers(data.invocationNamespace, data.action.fullyQualifiedName(true), data.action.rev, Some(instance), Some(data.container.containerId))}",
            "")
          parent ! Resumed(data)
          // the new queue may locates on an different scheduler, so recreate the activation client when necessary
          // since pekko port will no be used, we can put any value except 0 here
          data.clientProxy ! RequestActivation(
            newScheduler = Some(SchedulerEndpoints(job.schedulerHost, job.rpcPort, 10)))
          startSingleTimer(UnusedTimeoutName, StateTimeout, unusedTimeout)
          timedOut = false
        }
        .recover {
          case t: Throwable =>
            logging.error(this, s"Failed to resume container ${data.container.containerId}, error: $t")
            parent ! ResumeFailed(data)
            self ! ResumeFailed(data)
        }

      // always clean data in etcd regardless of success and failure
      dataManagementService ! UnregisterData(
        ContainerKeys.warmedContainers(
          data.invocationNamespace,
          data.action.fullyQualifiedName(false),
          data.revision,
          instance,
          data.container.containerId))
      goto(Running)
    case Event(StateTimeout, _: WarmData) =>
      self ! DetermineKeepContainer(0)
      stay
    case Event(DetermineKeepContainer(attempt), data: WarmData) =>
      getLiveContainerCount(data.invocationNamespace, data.action.fullyQualifiedName(false), data.revision)
        .flatMap(count => {
          getWarmedContainerLimit(data.invocationNamespace).map(warmedContainerInfo => {
            logging.info(
              this,
              s"Live container count: $count, warmed container keeping count configuration: ${warmedContainerInfo._1} in namespace: ${data.invocationNamespace}")
            if (count <= warmedContainerInfo._1) {
              self ! Keep(warmedContainerInfo._2)
            } else {
              self ! Remove
            }
          })
        })
        .recover({
          case t: Throwable =>
            logging.error(
              this,
              s"Failed to determine whether to keep or remove container on pause timeout for ${data.container.containerId}, retrying. Caused by: $t")
            if (attempt < 5) {
              startSingleTimer(DetermineKeepContainer.toString, DetermineKeepContainer(attempt + 1), 500.milli)
            } else {
              self ! Remove
            }
        })
      stay
    case Event(Keep(warmedContainerKeepingTimeout), data: WarmData) =>
      logging.info(
        this,
        s"This is the remaining container for ${data.action}. The container will stop after $warmedContainerKeepingTimeout.")
      startSingleTimer(KeepingTimeoutName, Remove, warmedContainerKeepingTimeout)
      stay
    case Event(Remove | GracefulShutdown, data: WarmData) =>
      cancelTimer(DetermineKeepContainer.toString)
      dataManagementService ! UnregisterData(
        ContainerKeys.warmedContainers(
          data.invocationNamespace,
          data.action.fullyQualifiedName(false),
          data.revision,
          instance,
          data.container.containerId))
      cleanUp(
        data.container,
        data.invocationNamespace,
        data.action.fullyQualifiedName(false),
        data.action.rev,
        Some(data.clientProxy))

    case x: Event if x.event != PingCache => delay
  }

  when(Removing, unusedTimeout) {
    // only if ClientProxy is closed, ContainerProxy stops. So it is important for ClientProxy to send ClientClosed.
    case Event(ClientClosed, _) if activeTargetBinding.isDefined && !runningActivations.isEmpty =>
      logging.info(this, s"waiting for ${runningActivations.size()} activation(s) before closing target binding")
      startSingleTimer(RunningActivationTimeoutName, ClientClosed, runningActivationTimeout)
      stay()

    case Event(ClientClosed, _) if activeTargetBinding.isDefined =>
      beginTargetBoundRemoval()
      stay()

    case Event(ClientClosed, _) => stop()

    case Event(TargetBoundContainerRemovalCompleted, _) => stop()

    case Event(TargetBoundContainerRemovalFailed(cause), _) =>
      logging.error(this, s"target binding close failed during container removal: ${cause.getMessage}")
      invokerHealthManager ! HealthMessage(state = false)
      stop()

    // even if any error occurs, it still waits for ClientClosed event in order to be stopped after the client is closed.
    case Event(t: FailureMessage, _) =>
      logging.error(this, s"unable to delete a container due to ${t}")

      stay

    case Event(StateTimeout, _) =>
      logging.error(this, s"could not receive ClientClosed for ${unusedTimeout}, so just stop the container proxy.")

      stop()

    case Event(Remove | GracefulShutdown, _) =>
      stay()

    case Event(DetermineKeepContainer(_), _) =>
      stay()
  }

  whenUnhandled {
    case Event(PingCache, data: WarmData) if data.action.exec.kind == TargetBindingProvider.ReusableConcurrencyKind =>
      logging.debug(this, "target-bound action code is owned by the concrete executor; skip worker DB cache ping")
      stay
    case Event(PingCache, data: WarmData) =>
      val actionId = data.action.fullyQualifiedName(false).toDocId.asDocInfo(data.revision)
      get(entityStore, actionId.id, actionId.rev, true, false).map(_ => {
        logging.debug(
          this,
          s"Refreshed function cache for action ${data.action} from container ${data.container.containerId}.")
      })
      stay
    case Event(PingCache, _) =>
      logging.debug(this, "Container is not warm, ignore function cache ping.")
      stay
  }

  onTransition {
    case _ -> Uninitialized     => unstashAll()
    case _ -> CreatingContainer => unstashAll()
    case _ -> ContainerCreated =>
      if (healtCheckConfig.enabled) {
        nextStateData.getContainer.foreach { c =>
          logging.info(this, s"enabling health ping for ${c.containerId.asString} on ContainerCreated")
          enableHealthPing(c)
        }
      }
      unstashAll()
    case _ -> CreatingClient => unstashAll()
    case _ -> ClientCreated  => unstashAll()
    case _ -> Running =>
      if (healtCheckConfig.enabled && healthPingActor.isDefined) {
        nextStateData.getContainer.foreach { c =>
          logging.info(this, s"disabling health ping for ${c.containerId.asString} on Running")
          disableHealthPing()
        }
      }
      unstashAll()
    case _ -> Paused   => startSingleTimer(IdleTimeoutName, StateTimeout, idleTimeout)
    case _ -> Removing => unstashAll()
  }

  initialize()

  /** Delays all incoming messages until unstashAll() is called */
  def delay = {
    stash()
    stay
  }

  /**
   * Only change the state if the currentState is not the newState.
   *
   * @param newState of the InvokerActor
   */
  private def gotoIfNotThere(newState: ProxyState) = {
    if (stateName == newState) stay() else goto(newState)
  }

  /**
   * Clean up all meta data of invoking action
   *
   * @param container the container to destroy
   * @param fqn the action to stop
   * @param clientProxy the client to destroy
   * @return
   */
  private def cleanUp(container: Container,
                      invocationNamespace: String,
                      fqn: FullyQualifiedEntityName,
                      revision: DocRevision,
                      clientProxy: Option[ActorRef]): State = {
    cancelTimer(PingCacheName)
    dataManagementService ! UnregisterData(
      s"${ContainerKeys.existingContainers(invocationNamespace, fqn, revision, Some(instance), Some(container.containerId))}")

    cleanUp(container, clientProxy)
  }

  private def cleanUp(container: Container, clientProxy: Option[ActorRef], replacePrewarm: Boolean = true): State = {
    context.parent ! ContainerRemoved(replacePrewarm)
    if (activeTargetBinding.isDefined) {
      pendingTargetBoundRemoval = Some(container -> (stateName == Paused))
    } else {
      val unpause = stateName match {
        case Paused => container.resume()(TransactionId.invokerNanny)
        case _      => Future.successful(())
      }
      unpause.andThen {
        case Success(_) => destroyContainer(container)
        case Failure(t) =>
          // docker may hang when try to remove a paused container, so we shouldn't remove it
          logging.error(this, s"Failed to resume container ${container.containerId}, error: $t")
      }
    }
    clientProxy match {
      case Some(clientProxy) => clientProxy ! StopClientProxy
      case None              => self ! ClientClosed
    }
    gotoIfNotThere(Removing)
  }

  private def beginTargetBoundRemoval(): Unit = {
    if (!targetBoundRemovalStarted) {
      (pendingTargetBoundRemoval, activeTargetBinding) match {
        case (Some((container, wasPaused)), Some((target, binding))) =>
          targetBoundRemovalStarted = true
          val unpause =
            if (wasPaused) container.resume()(TransactionId.invokerNanny)
            else Future.successful(())
          unpause
            .flatMap(_ => targetBindingProvider.closeBinding(target, binding))
            .transformWith {
              case Success(_) => destroyContainer(container)
              case Failure(closeFailure) =>
                destroyContainer(container).transform(_ => Failure(closeFailure))
            }
            .map(_ => TargetBoundContainerRemovalCompleted)
            .recover { case t => TargetBoundContainerRemovalFailed(t) }
            .pipeTo(self)
        case _ =>
          self ! TargetBoundContainerRemovalFailed(
            new IllegalStateException("target-bound removal state is incomplete"))
      }
    }
  }

  /**
   * Destroys the container
   *
   * @param container the container to destroy
   */
  private def destroyContainer(container: Container) = {
    container
      .destroy()(TransactionId.invokerNanny)
      .andThen {
        case Failure(t) =>
          logging.error(this, s"Failed to destroy container: ${container.containerId.asString} caused by ${t}")
      }
  }

  private def handleActivationMessage(msg: ActivationMessage, action: ExecutableWhiskAction): Future[RunActivation] = {
    implicit val transid = msg.transid
    logging.info(this, s"received a message ${msg.activationId} for ${msg.action} in $stateName")
    if (!namespaceBlacklist.isBlacklisted(msg.user)) {
      logging.debug(this, s"namespace ${msg.user.namespace.name} is not in the namespaceBlacklist")
      val namespace = msg.action.path
      val name = msg.action.name
      val actionid = FullyQualifiedEntityName(namespace, name).toDocId.asDocInfo(msg.revision)
      val subject = msg.user.subject

      logging.debug(this, s"${actionid.id} $subject ${msg.activationId}")

      // set trace context to continue tracing
      WhiskTracerProvider.tracer.setTraceContext(transid, msg.traceContext)

      // caching is enabled since actions have revision id and an updated
      // action will not hit in the cache due to change in the revision id;
      // if the doc revision is missing, then bypass cache
      if (actionid.rev == DocRevision.empty)
        logging.warn(this, s"revision was not provided for ${actionid.id}")

      val runActivation =
        if (action.exec.kind == TargetBindingProvider.ReusableConcurrencyKind) {
          TargetBoundActivationContent.parseTargetDispatch(msg.content) match {
            case Right(dispatch) if dispatch.exactRevision == msg.revision && msg.revision == action.rev =>
              action.limits.checkLimits(msg.user)
              Future.successful(RunActivation(action, msg))
            case Right(_) =>
              Future.failed(
                new IllegalStateException("target-bound activation revision does not match container action"))
            case Left(error) => Future.failed(new IllegalStateException(error.targetReencryptionError))
          }
        } else {
          get(entityStore, actionid.id, actionid.rev, actionid.rev != DocRevision.empty, false)
            .flatMap { fetchedAction =>
              // action that exceed the limit cannot be executed
              fetchedAction.limits.checkLimits(msg.user)
              fetchedAction.toExecutableWhiskAction match {
                case Some(executable) =>
                  Future.successful(RunActivation(executable, msg))
                case None =>
                  logging.error(
                    this,
                    s"non-executable action reached the invoker ${fetchedAction.fullyQualifiedName(false)}")
                  Future.failed(new IllegalStateException("non-executable action reached the invoker"))
              }
            }
        }

      runActivation
        .recoverWith {
          case DocumentRevisionMismatchException(_)
              if action.exec.kind != TargetBindingProvider.ReusableConcurrencyKind =>
            // if revision is mismatched, the action may have been updated,
            // so try again with the latest code
            logging.warn(
              this,
              s"msg ${msg.activationId} for ${msg.action} in $stateName is updated, fetching latest code")
            handleActivationMessage(msg.copy(revision = DocRevision.empty), action)
          case t =>
            // If the action cannot be found, the user has concurrently deleted it,
            // making this an application error. All other errors are considered system
            // errors and should cause the invoker to be considered unhealthy.
            val response =
              if (action.exec.kind == TargetBindingProvider.ReusableConcurrencyKind) {
                logging.error(this, s"target-bound activation contract rejected: ${t.getMessage}")
                ExecutionResponse.whiskError("target-bound activation contract rejected")
              } else
                t match {
                  case _: NoDocumentException =>
                    ExecutionResponse.applicationError(Messages.actionRemovedWhileInvoking)
                  case e: ActionLimitsException =>
                    ExecutionResponse.applicationError(e.getMessage) // return generated failed message
                  case _: DocumentTypeMismatchException | _: DocumentUnreadable =>
                    ExecutionResponse.whiskError(Messages.actionMismatchWhileInvoking)
                  case e: Throwable =>
                    logging.error(this, s"An unknown DB connection error occurred while fetching an action: $e.")
                    ExecutionResponse.whiskError(Messages.actionFetchErrorWhileInvoking)
                }
            val errMsg = s"Error to fetch action ${msg.action} for msg ${msg.activationId}, error is ${t.getMessage}"
            logging.error(this, errMsg)

            val context = UserContext(msg.user)
            val activation = generateFallbackActivation(action, msg, response)
            sendActiveAck(
              transid,
              activation,
              msg.blocking,
              msg.rootControllerIndex,
              msg.user.namespace.uuid,
              CombinedCompletionAndResultMessage(transid, activation, instance))
            guardedStoreActivation(msg.transid, activation, msg, action.exec.kind, msg.blocking, context)

            // in case action is removed container proxy should be terminated
            Future.failed(new IllegalStateException(errMsg))
        }
    } else {
      // Iff the current namespace is blacklisted, an active-ack is only produced to keep the loadbalancer protocol
      // Due to the protective nature of the blacklist, a database entry is not written.
      val activation =
        generateFallbackActivation(action, msg, ExecutionResponse.applicationError(Messages.namespacesBlacklisted))
      sendActiveAck(
        msg.transid,
        activation,
        false,
        msg.rootControllerIndex,
        msg.user.namespace.uuid,
        CombinedCompletionAndResultMessage(msg.transid, activation, instance))
      logging.warn(
        this,
        s"namespace ${msg.user.namespace.name} was blocked in containerProxy, complete msg ${msg.activationId} with error.")
      Future.failed(new IllegalStateException(s"namespace ${msg.user.namespace.name} was blocked in containerProxy."))
    }

  }

  private def enableHealthPing(c: Container) = {
    val hpa = healthPingActor.getOrElse {
      logging.info(this, s"creating health ping actor for ${c.addr.asString()}")
      val hp = context.actorOf(
        TCPPingClient
          .props(tcp, c.toString(), healtCheckConfig, new InetSocketAddress(c.addr.host, c.addr.port)))
      healthPingActor = Some(hp)
      hp
    }
    hpa ! HealthPingEnabled(true)
  }

  private def disableHealthPing() = {
    healthPingActor.foreach(_ ! HealthPingEnabled(false))
  }

  def fallbackActivationForReschedulingData(data: ReschedulingData): Unit = {
    val context = UserContext(data.resumeRun.msg.user)
    val activation =
      generateFallbackActivation(data.action, data.resumeRun.msg, ExecutionResponse.whiskError(Messages.abnormalRun))

    sendActiveAck(
      data.resumeRun.msg.transid,
      activation,
      data.resumeRun.msg.blocking,
      data.resumeRun.msg.rootControllerIndex,
      data.resumeRun.msg.user.namespace.uuid,
      CombinedCompletionAndResultMessage(data.resumeRun.msg.transid, activation, instance))

    guardedStoreActivation(
      data.resumeRun.msg.transid,
      activation,
      data.resumeRun.msg,
      data.action.exec.kind,
      data.resumeRun.msg.blocking,
      context)
  }

  /**
   * Runs the job, initialize first if necessary.
   * Completes the job by:
   * 1. sending an activate ack,
   * 2. fetching the logs for the run,
   * 3. indicating the resource is free to the parent pool,
   * 4. recording the result to the data store
   *
   * @param container the container to run the job on
   * @param job the job to run
   * @return a future completing after logs have been collected and
   *         added to the WhiskActivation
   */
  private def initializeAndRunActivation(
    container: Container,
    clientProxy: ActorRef,
    action: ExecutableWhiskAction,
    msg: ActivationMessage,
    resumeRun: Option[RunActivation] = None)(implicit tid: TransactionId): Future[WhiskActivation] = {
    // Add the activation to runningActivations set
    runningActivations.put(msg.activationId.asString, true)

    val actionTimeout = action.limits.timeout.duration

    val targetDispatch =
      if (action.exec.kind == TargetBindingProvider.ReusableConcurrencyKind) {
        TargetBoundActivationContent.parseTargetDispatch(msg.content) match {
          case Right(value) => Some(value)
          case Left(error)  => throw new IllegalStateException(error.targetReencryptionError)
        }
      } else None

    val (env, parameters) = targetDispatch match {
      case Some(dispatch) =>
        Map.empty[String, JsValue] -> JsObject(
          "__reusable_protected_input_envelope" -> JsString(
            Base64.getEncoder.encodeToString(dispatch.targetInput.bytes.toArray)))
      case None => ContainerProxy.partitionArguments(msg.content, msg.initArgs)
    }

    val environment = Map(
      "namespace" -> msg.user.namespace.name.toJson,
      "action_name" -> msg.action.qualifiedNameWithLeadingSlash.toJson,
      "action_version" -> msg.action.version.toJson,
      "activation_id" -> msg.activationId.toString.toJson,
      "transaction_id" -> msg.transid.id.toJson)

    // if the action requests the api key to be injected into the action context, add it here;
    // treat a missing annotation as requesting the api key for backward compatibility
    val authEnvironment = {
      if (targetDispatch.isEmpty &&
          action.annotations.isTruthy(Annotations.ProvideApiKeyAnnotationName, valueForNonExistent = true)) {
        msg.user.authkey.toEnvironment.fields
      } else Map.empty
    }

    val targetPreconditions: Future[Unit] = targetDispatch match {
      case Some(dispatch) =>
        val actualWarm = stateData.isInstanceOf[WarmData]
        val activeBinding = activeTargetBinding.map(_._2.id)
        if (dispatch.exactRevision != msg.revision || dispatch.exactRevision != action.rev) {
          Future.failed(new IllegalStateException("target-bound action revision changed before execution"))
        } else if (activeBinding != Some(dispatch.targetBindingId)) {
          Future.failed(
            new IllegalStateException("target-bound activation does not match the concrete container binding"))
        } else if (dispatch.warmed != actualWarm) {
          Future.failed(new IllegalStateException("target-bound warmed state does not match the concrete container"))
        } else Future.successful(())
      case None => Future.successful(())
    }

    // Only initialize iff we haven't yet warmed the container
    val initialize = targetPreconditions.flatMap { _ =>
      stateData match {
        case _: WarmData =>
          Future.successful(None)
        case _ =>
          val owEnv = (authEnvironment ++ environment ++ Map(
            "deadline" -> (Instant.now.toEpochMilli + actionTimeout.toMillis).toString.toJson)) map {
            case (key, value) => "__OW_" + key.toUpperCase -> value
          }
          emitC1TimingEvent("OW500", "openwhisk_container_initialize_enter", msg)
          val initialization = targetDispatch match {
            case Some(dispatch) =>
              container.initializeTargetBound(
                FunctionPullingContainerProxy.targetBoundInitializer(action, env ++ owEnv, dispatch),
                actionTimeout,
                action.limits.concurrency.maxConcurrent)
            case None =>
              container.initialize(
                action.containerInitializer(env ++ owEnv),
                actionTimeout,
                action.limits.concurrency.maxConcurrent)
          }
          initialization
            .andThen {
              case _ => emitC1TimingEvent("OW510", "openwhisk_container_initialize_exit", msg)
            }
            .map(Some(_))
      }
    }

    val activation: Future[WhiskActivation] = initialize
      .flatMap { initInterval =>
        // immediately setup warmedData for use (before first execution) so that concurrent actions can use it asap
        if (initInterval.isDefined) {
          stateData match {
            case _: InitializedData =>
              self ! InitCodeCompleted(
                WarmData(container, msg.user.namespace.name.asString, action, msg.revision, Instant.now, clientProxy))

            case _ =>
              Future.failed(new IllegalStateException("lease does not exist"))
          }
        }
        val env = authEnvironment ++ environment ++ Map(
          // compute deadline on invoker side avoids discrepancies inside container
          // but potentially under-estimates actual deadline
          "deadline" -> (Instant.now.toEpochMilli + actionTimeout.toMillis).toString.toJson)

        val activationId = msg.activationId.asString
        val injectInternalReschedule =
          c1InternalRescheduleInjectionEnabled &&
            resumeRun.isDefined &&
            c1InternalRescheduleRequested(parameters) &&
            (FunctionPullingContainerProxy.c1InternalRescheduleInjectedActivations
              .putIfAbsent(activationId, java.lang.Boolean.TRUE) eq null)

        val runResult =
          if (injectInternalReschedule) {
            logging.warn(
              this,
              s"C1_INTERNAL_RESCHEDULE_INJECTION|activation_id=${c1TimingSanitize(activationId)}|container_id=${c1TimingSanitize(
                container.containerId.asString)}|reason=$c1InternalRescheduleInjectionReason|status=injected_once")(
              msg.transid)
            Future.failed(ContainerHealthError(msg.transid, c1InternalRescheduleInjectionReason))
          } else {
            FunctionPullingContainerProxy.withC1NativeBackendPressureExecutionBoundaries(
              isBackendPressureActivation(msg),
              action.exec.kind,
              (eventCode, boundaryName) => emitC1TimingEvent(eventCode, boundaryName, msg)) {
              targetDispatch match {
                case Some(_) =>
                  container.runTargetBound(
                    parameters,
                    env.toJson.asJsObject,
                    actionTimeout,
                    action.limits.concurrency.maxConcurrent,
                    msg.user.limits.allowedMaxPayloadSize,
                    msg.user.limits.allowedTruncationSize,
                    resumeRun.isDefined)(msg.transid)
                case None =>
                  container.run(
                    parameters,
                    env.toJson.asJsObject,
                    actionTimeout,
                    action.limits.concurrency.maxConcurrent,
                    msg.user.limits.allowedMaxPayloadSize,
                    msg.user.limits.allowedTruncationSize,
                    resumeRun.isDefined)(msg.transid)
              }
            }
          }

        runResult
          .map {
            case (runInterval, response) =>
              val initRunInterval = initInterval
                .map(i => Interval(runInterval.start.minusMillis(i.duration.toMillis), runInterval.end))
                .getOrElse(runInterval)
              val effectiveResponse = targetDispatch
                .map(dispatch => FunctionPullingContainerProxy.targetBoundRuntimeResponse(dispatch, response))
                .getOrElse(response)
              val whiskActivation = constructWhiskActivation(
                action,
                msg,
                initInterval,
                initRunInterval,
                runInterval.duration >= actionTimeout,
                effectiveResponse)
              emitC1TimingEvent("N700", "native_worker_result_ready", msg)
              whiskActivation
          }
      }
      .recoverWith {
        case h: ContainerHealthError if resumeRun.isDefined =>
          // health error occurs
          logging.error(this, s"caught healthchek check error while running activation")
          val resumedRun =
            if (h.msg == c1InternalRescheduleInjectionReason) {
              resumeRun.get.copy(msg = c1ConsumeInternalRescheduleRequest(resumeRun.get.msg))
            } else {
              resumeRun.get
            }
          Future.failed(ContainerHealthErrorWithResumedRun(h.tid, h.msg, resumedRun))

        case InitializationError(interval, response) =>
          Future.successful(
            constructWhiskActivation(
              action,
              msg,
              Some(interval),
              interval,
              interval.duration >= actionTimeout,
              response))

        case t =>
          // Actually, this should never happen - but we want to make sure to not miss a problem
          logging.error(this, s"caught unexpected error while running activation: $t")
          Future.successful(
            constructWhiskActivation(
              action,
              msg,
              None,
              Interval.zero,
              false,
              ExecutionResponse.whiskError(Messages.abnormalRun)))
      }

    val splitAckMessagesPendingLogCollection = collectLogs.logsToBeCollected(action)
    // Sending an active ack is an asynchronous operation. The result is forwarded as soon as
    // possible for blocking activations so that dependent activations can be scheduled. The
    // completion message which frees a load balancer slot is sent after the active ack future
    // completes to ensure proper ordering.
    val sendResult = if (msg.blocking) {
      activation.map { result =>
        val ackMsg =
          if (splitAckMessagesPendingLogCollection) ResultMessage(tid, result)
          else CombinedCompletionAndResultMessage(tid, result, instance)
        emitC1TimingEvent("OW800", "openwhisk_result_notify_submit", msg)
        emitC1TimingEvent("N800", "native_worker_result_notify_submit", msg)
        sendActiveAck(tid, result, msg.blocking, msg.rootControllerIndex, msg.user.namespace.uuid, ackMsg)
      }
    } else {
      // For non-blocking request, do not forward the result.
      if (splitAckMessagesPendingLogCollection) Future.successful(())
      else
        activation.map { result =>
          val ackMsg = CompletionMessage(tid, result, instance)
          emitC1TimingEvent("OW800", "openwhisk_result_notify_submit", msg)
          emitC1TimingEvent("N800", "native_worker_result_notify_submit", msg)
          sendActiveAck(tid, result, msg.blocking, msg.rootControllerIndex, msg.user.namespace.uuid, ackMsg)
        }
    }

    activation.foreach { activation =>
      emitC1BackendPressureWorkloadTiming(msg, activation)
      emitC1BackendPressureAsynCSEvidence(msg, activation)
      val healthMessage = HealthMessage(!activation.response.isWhiskError)
      invokerHealthManager ! healthMessage
    }

    val context = UserContext(msg.user)

    // Adds logs to the raw activation.
    val activationWithLogs: Future[Either[ActivationLogReadingError, WhiskActivation]] = activation
      .flatMap { activation =>
        // Skips log collection entirely, if the limit is set to 0
        if (action.limits.logs.asMegaBytes == 0.MB) {
          Future.successful(Right(activation))
        } else {
          val start = tid.started(this, LoggingMarkers.INVOKER_COLLECT_LOGS, logLevel = InfoLevel)
          collectLogs(tid, msg.user, activation, container, action)
            .andThen {
              case Success(_) => tid.finished(this, start)
              case Failure(t) => tid.failed(this, start, s"reading logs failed: $t")
            }
            .map(logs => Right(activation.withLogs(logs)))
            .recover {
              case LogCollectingException(logs) =>
                Left(ActivationLogReadingError(activation.withLogs(logs)))
              case _ =>
                Left(ActivationLogReadingError(activation.withLogs(ActivationLogs(Vector(Messages.logFailure)))))
            }
        }
      }

    activationWithLogs
      .map(_.fold(_.activation, identity))
      .foreach { activation =>
        // Sending the completion message to the controller after the active ack ensures proper ordering
        // (result is received before the completion message for blocking invokes).
        if (splitAckMessagesPendingLogCollection) {
          sendResult.onComplete(_ => {
            if (!msg.blocking) {
              emitC1TimingEvent("OW800", "openwhisk_result_notify_submit", msg)
              emitC1TimingEvent("N800", "native_worker_result_notify_submit", msg)
            }
            sendActiveAck(
              tid,
              activation,
              msg.blocking,
              msg.rootControllerIndex,
              msg.user.namespace.uuid,
              CompletionMessage(tid, activation, instance))
          })
        }

        // Storing the record. Entirely asynchronous and not waited upon.
        guardedStoreActivation(tid, activation, msg, action.exec.kind, msg.blocking, context)
      }

    // Disambiguate activation errors and transform the Either into a failed/successful Future respectively.
    activationWithLogs
      .andThen {
        // remove activationId from runningActivations in any case
        case _ => runningActivations.remove(msg.activationId.asString)
      }
      .flatMap {
        case Right(act) if !act.response.isSuccess && !act.response.isApplicationError =>
          Future.failed(ActivationUnsuccessfulError(act))
        case Left(error) => Future.failed(error)
        case Right(act)  => Future.successful(act)
      }
  }

  /** Generates an activation with zero runtime. Usually used for error cases */
  private def generateFallbackActivation(action: ExecutableWhiskAction,
                                         msg: ActivationMessage,
                                         response: ExecutionResponse): WhiskActivation = {
    val now = Instant.now
    val causedBy = if (msg.causedBySequence) {
      Some(Parameters(WhiskActivation.causedByAnnotation, JsString(Exec.SEQUENCE)))
    } else None

    WhiskActivation(
      activationId = msg.activationId,
      namespace = msg.user.namespace.name.toPath,
      subject = msg.user.subject,
      cause = msg.cause,
      name = msg.action.name,
      version = msg.action.version.getOrElse(SemVer()),
      start = now,
      end = now,
      duration = Some(0),
      response = response,
      annotations = {
        Parameters(WhiskActivation.pathAnnotation, JsString(msg.action.copy(version = None).asString)) ++
          Parameters(WhiskActivation.kindAnnotation, JsString(action.exec.kind)) ++
          causedBy
      })
  }

}

object FunctionPullingContainerProxy {
  val ReusableConcurrencySkipActivationStoreEnv = "REUSABLE_CONCURRENCY_SKIP_ACTIVATION_STORE"
  private[containerpool] val ReusableConcurrencyStoreSkipReason = "reusable_concurrency_profile_store_skip"
  private val P2ResultEnvelopeField = "__reusable_protected_result_envelope"
  private val P2RequestIdHashField = "__reusable_request_id_hash"
  private val P2TargetBindingField = "__reusable_target_binding_id"

  private[containerpool] def reusableConcurrencyStoreSkipEnabled(environment: Map[String, String]): Boolean =
    environment
      .get(ReusableConcurrencySkipActivationStoreEnv)
      .exists(value => Set("1", "true", "yes").contains(value.trim.toLowerCase))

  private[containerpool] def reusableConcurrencyStoreSkipReason(enabled: Boolean, actionKind: String): Option[String] =
    if (enabled && actionKind == TargetBindingProvider.ReusableConcurrencyKind) {
      Some(ReusableConcurrencyStoreSkipReason)
    } else {
      None
    }

  private[containerpool] def storeActivationUnlessSkipped(skipReason: Option[String])(
    store: => Future[Any]): Future[Any] =
    skipReason.fold(store)(_ => Future.successful(()))

  private[containerpool] def targetBoundInitializer(action: ExecutableWhiskAction,
                                                    environment: Map[String, JsValue],
                                                    dispatch: TargetBoundActivationContent.TargetDispatch): JsObject = {
    val base = action.containerInitializer(environment)
    JsObject(
      base.fields ++ Map(
        "code" -> JsString.empty,
        "protected_code_envelope" -> JsString(Base64.getEncoder.encodeToString(dispatch.targetCode.get.bytes.toArray)),
        "annotations" -> JsObject.empty))
  }

  private[containerpool] def targetBoundRuntimeResponse(dispatch: TargetBoundActivationContent.TargetDispatch,
                                                        response: ExecutionResponse): ExecutionResponse = {
    if (!response.isSuccess) {
      response
    } else {
      val parsed = for {
        outer <- response.result match {
          case Some(value: JsObject) => Right(value)
          case _                     => Left("protected runtime response is not an object")
        }
        runtimeSuccess <- outer.fields.get("success") match {
          case Some(JsBoolean(value)) => Right(value)
          case _                      => Left("protected runtime response is missing success")
        }
        statusCode <- outer.fields.get("status_code") match {
          case Some(JsNumber(value)) if value.isValidInt => Right(value.toInt)
          case _                                         => Left("protected runtime response is missing status_code")
        }
        _ <- Either.cond(runtimeSuccess && statusCode == 0, (), "protected runtime reported an application failure")
        result <- outer.fields.get("result") match {
          case Some(value: JsObject) => Right(value)
          case _                     => Left("protected runtime result is not an object")
        }
        encodedEnvelope <- result.fields.get(P2ResultEnvelopeField) match {
          case Some(JsString(value)) => Right(value)
          case _                     => Left("protected runtime result is missing its T2G envelope")
        }
        envelopeBytes <- try Right(
          org.apache.pekko.util.ByteString.fromArray(Base64.getDecoder.decode(encodedEnvelope)))
        catch {
          case _: IllegalArgumentException => Left("protected runtime result envelope is not base64")
        }
        envelope <- ProtectedEnvelopeV1.decode(envelopeBytes)
        requestIdHash <- result.fields.get(P2RequestIdHashField) match {
          case Some(JsString(value)) if value.matches("[0-9a-f]{64}") => Right(value)
          case _                                                      => Left("protected runtime result has an invalid request correlation")
        }
        targetBindingId <- result.fields.get(P2TargetBindingField) match {
          case Some(JsNumber(value)) if value.isValidLong => Right(value.toLong)
          case Some(JsString(value)) =>
            try Right(value.toLong)
            catch { case _: NumberFormatException => Left("protected runtime result has an invalid target binding") }
          case _ => Left("protected runtime result is missing its target binding")
        }
        expectedRequestIdHash = dispatch.targetInput.correlationHash.map(byte => f"${byte & 0xff}%02x").mkString
        _ <- Either.cond(
          targetBindingId == dispatch.targetBindingId &&
            envelope.bindingId == dispatch.targetBindingId &&
            envelope.kind == ProtectedObjectKind.Result &&
            envelope.direction == ProtectedEnvelopeDirection.TargetToGateway &&
            requestIdHash == expectedRequestIdHash &&
            envelope.correlationHash == dispatch.targetInput.correlationHash,
          (),
          "protected runtime result does not match its target/request correlation")
      } yield
        TargetBoundActivationContent.targetResult(
          TargetBoundActivationContent
            .TargetResult(dispatch.sourceBindingId, dispatch.targetBindingId, requestIdHash, envelope))

      parsed match {
        case Right(result) => ExecutionResponse.success(Some(result))
        case Left(message) => ExecutionResponse.whiskError(message)
      }
    }
  }

  private[containerpool] def withC1NativeBackendPressureExecutionBoundaries[T](
    markedBackendPressure: Boolean,
    actionKind: String,
    emit: (String, String) => Unit)(run: => Future[T])(implicit executionContext: ExecutionContext): Future[T] = {
    if (markedBackendPressure && actionKind == "nodejs:20") {
      emit("N420", "native_action_container_run_enter")
      Try(run) match {
        case Success(future) =>
          future.andThen {
            case _ => emit("N430", "native_action_container_run_exit")
          }
        case Failure(t) =>
          emit("N430", "native_action_container_run_exit")
          Future.failed(t)
      }
    } else {
      run
    }
  }

  private val c1AsynCSProducerEventCodes =
    Set("A200", "A210", "A300", "A310", "A320", "A330", "A340", "A350", "A400", "A410")
  private val c1AsynCSTraceEvidenceFields = Seq(
    "worker_started_this_invocation",
    "worker_restart_reason",
    "kms_contacted",
    "enclave_key_cache_hit",
    "worker_invoke_rc",
    "container_hostname",
    "crypto_profile",
    "key_ciphertext_format",
    "function_cache_hit",
    "cfunc_decrypt_executed",
    "function_cache_bytes",
    "payload_bytes_consumed",
    "payload_sha256",
    "workload_timing_schema",
    "workload_core_cycles",
    "output_kem_cycles",
    "output_aes_gcm_cycles",
    "tsc_hz",
    "tsc_frequency_method",
    "workload_core_duration_ns",
    "output_kem_duration_ns",
    "output_aes_gcm_duration_ns")

  private def c1EvidenceValue(value: JsValue): Option[String] = value match {
    case JsString(content)  => Some(content)
    case JsNumber(content)  => Some(content.toString)
    case JsBoolean(content) => Some(content.toString)
    case _                  => None
  }

  private def c1EvidenceField(fields: Map[String, JsValue], key: String): Option[String] =
    fields.get(key).flatMap(c1EvidenceValue)

  private def c1EvidenceSanitize(value: String): String =
    value.replace('|', '_').replace('\n', ' ').replace('\r', ' ')

  private def c1EvidenceLine(prefix: String, fields: Seq[(String, String)]): String =
    s"$prefix|${fields.map { case (key, value) => s"$key=${c1EvidenceSanitize(value)}" }.mkString("|")}"

  private[containerpool] def c1BackendPressureAsynCSEvidenceLines(markedBackendPressure: Boolean,
                                                                  result: Option[JsValue],
                                                                  activationId: String,
                                                                  node: String,
                                                                  transactionId: String): Seq[String] = {
    if (!markedBackendPressure) {
      Seq.empty
    } else {
      result.toSeq.flatMap {
        case JsObject(resultFields) =>
          val errorFields = resultFields.get(ExecutionResponse.ERROR_FIELD).collect {
            case JsObject(fields) => fields
          }
          val trace = resultFields
            .get("trace")
            .orElse(errorFields.flatMap(_.get("trace")))
            .collect { case JsObject(fields) => fields }
          val actualRid = c1EvidenceField(resultFields, "rid").orElse(errorFields.flatMap(c1EvidenceField(_, "rid")))

          trace.toSeq.flatMap { traceFields =>
            traceFields.get("producer_timing_events") match {
              case Some(JsArray(events)) =>
                val producerEvents = events
                  .collect {
                    case JsObject(fields)
                        if c1EvidenceField(fields, "event_code").exists(c1AsynCSProducerEventCodes.contains) =>
                      fields
                  }
                  .take(c1AsynCSProducerEventCodes.size)
                val firstEvent = producerEvents.headOption.getOrElse(Map.empty[String, JsValue])
                val logicalRequestId = c1EvidenceField(firstEvent, "logical_request_id").getOrElse("")
                val attemptId = c1EvidenceField(firstEvent, "attempt_id").getOrElse("")
                val resultEvidence = c1EvidenceLine(
                  "C1_BACKEND_PRESSURE_ASYNCS_RESULT",
                  Seq(
                    "activation_id" -> activationId,
                    "logical_request_id" -> logicalRequestId,
                    "attempt_id" -> attemptId,
                    "actual_rid" -> actualRid.getOrElse(""),
                    "node" -> node,
                    "producer_event_count" -> producerEvents.size.toString) ++
                    c1AsynCSTraceEvidenceFields.flatMap(key => c1EvidenceField(traceFields, key).map(key -> _)))
                val timingEvents = producerEvents.map { eventFields =>
                  val attrs = eventFields.get("attrs").collect { case JsObject(fields) => fields }.getOrElse(Map.empty)
                  c1EvidenceLine(
                    "C1TIMING_EVENT",
                    Seq(
                      "event_code" -> c1EvidenceField(eventFields, "event_code").getOrElse(""),
                      "event_seq" -> c1EvidenceField(eventFields, "event_seq").getOrElse(""),
                      "activation_id" -> activationId,
                      "boundary_name" -> c1EvidenceField(attrs, "boundary").getOrElse(""),
                      "logical_request_id" -> c1EvidenceField(eventFields, "logical_request_id").getOrElse(""),
                      "attempt_id" -> c1EvidenceField(eventFields, "attempt_id").getOrElse(""),
                      "node" -> node,
                      "process" -> c1EvidenceField(eventFields, "process").getOrElse(""),
                      "pid" -> c1EvidenceField(eventFields, "pid").getOrElse(""),
                      "tid" -> c1EvidenceField(eventFields, "tid").filter(_.nonEmpty).getOrElse(transactionId),
                      "unix_ns" -> c1EvidenceField(eventFields, "unix_ns").getOrElse(""),
                      "mono_ns" -> c1EvidenceField(eventFields, "mono_ns").getOrElse(""),
                      "clock_domain" -> c1EvidenceField(eventFields, "clock_domain").getOrElse(""),
                      "status" -> c1EvidenceField(eventFields, "status").getOrElse("observed")))
                }
                resultEvidence +: timingEvents
              case _ => Seq.empty
            }
          }
        case _ => Seq.empty
      }
    }
  }

  private[containerpool] val c1InternalRescheduleInjectedActivations =
    new ConcurrentHashMap[String, java.lang.Boolean]

  def props(factory: (TransactionId,
                      String,
                      ImageName,
                      Boolean,
                      ByteSize,
                      Int,
                      Option[Double],
                      Option[ExecutableWhiskAction]) => Future[Container],
            entityStore: ArtifactStore[WhiskEntity],
            namespaceBlacklist: NamespaceBlacklist,
            get: (ArtifactStore[WhiskEntity], DocId, DocRevision, Boolean, Boolean) => Future[WhiskAction],
            dataManagementService: ActorRef,
            clientProxyFactory: (ActorRefFactory,
                                 String,
                                 FullyQualifiedEntityName,
                                 DocRevision,
                                 String,
                                 Int,
                                 ContainerId,
                                 Option[Long]) => ActorRef,
            ack: ActiveAck,
            store: (TransactionId, WhiskActivation, Boolean, UserContext) => Future[Any],
            collectLogs: LogsCollector,
            getLiveContainerCount: (String, FullyQualifiedEntityName, DocRevision) => Future[Long],
            getWarmedContainerLimit: (String) => Future[(Int, FiniteDuration)],
            instance: InvokerInstanceId,
            invokerHealthManager: ActorRef,
            poolConfig: ContainerPoolConfig,
            timeoutConfig: ContainerProxyTimeoutConfig,
            healthCheckConfig: ContainerProxyHealthCheckConfig =
              loadConfigOrThrow[ContainerProxyHealthCheckConfig](ConfigKeys.containerProxyHealth),
            tcp: Option[ActorRef] = None,
            targetBindingProvider: TargetBindingProvider = TargetBindingProvider.Disabled)(
    implicit actorSystem: ActorSystem,
    logging: Logging) =
    Props(
      new FunctionPullingContainerProxy(
        factory,
        entityStore,
        namespaceBlacklist,
        get,
        dataManagementService,
        clientProxyFactory,
        ack,
        store,
        collectLogs,
        getLiveContainerCount,
        getWarmedContainerLimit,
        instance,
        invokerHealthManager,
        poolConfig,
        timeoutConfig,
        healthCheckConfig,
        tcp,
        targetBindingProvider))

  private val containerCount = new Counter

  /**
   * Generates a unique container name.
   *
   * @param prefix the container name's prefix
   * @param suffix the container name's suffix
   * @return a unique container name
   */
  def containerName(instance: InvokerInstanceId, prefix: String, suffix: String): String = {
    def isAllowed(c: Char): Boolean = c.isLetterOrDigit || c == '_'

    val sanitizedPrefix = prefix.filter(isAllowed)
    val sanitizedSuffix = suffix.filter(isAllowed)

    s"${ContainerFactory.containerNamePrefix(instance)}_${containerCount.next()}_${sanitizedPrefix}_${sanitizedSuffix}"
  }

  /**
   * Creates a WhiskActivation ready to be sent via active ack.
   *
   * @param job the job that was executed
   * @param interval the time it took to execute the job
   * @param response the response to return to the user
   * @return a WhiskActivation to be sent to the user
   */
  def constructWhiskActivation(action: ExecutableWhiskAction,
                               msg: ActivationMessage,
                               initInterval: Option[Interval],
                               totalInterval: Interval,
                               isTimeout: Boolean,
                               response: ExecutionResponse) = {

    val causedBy = if (msg.causedBySequence) {
      Some(Parameters(WhiskActivation.causedByAnnotation, JsString(Exec.SEQUENCE)))
    } else None

    val waitTime = {
      val end = initInterval.map(_.start).getOrElse(totalInterval.start)
      Parameters(WhiskActivation.waitTimeAnnotation, Interval(msg.transid.meta.start, end).duration.toMillis.toJson)
    }

    val initTime = {
      initInterval.map(initTime => Parameters(WhiskActivation.initTimeAnnotation, initTime.duration.toMillis.toJson))
    }

    val binding =
      msg.action.binding.map(f => Parameters(WhiskActivation.bindingAnnotation, JsString(f.asString)))

    WhiskActivation(
      activationId = msg.activationId,
      namespace = msg.user.namespace.name.toPath,
      subject = msg.user.subject,
      cause = msg.cause,
      name = action.name,
      version = action.version,
      start = totalInterval.start,
      end = totalInterval.end,
      duration = Some(totalInterval.duration.toMillis),
      response = response,
      annotations = {
        Parameters(WhiskActivation.limitsAnnotation, action.limits.toJson) ++
          Parameters(WhiskActivation.pathAnnotation, JsString(action.fullyQualifiedName(false).asString)) ++
          Parameters(WhiskActivation.kindAnnotation, JsString(action.exec.kind)) ++
          Parameters(WhiskActivation.timeoutAnnotation, JsBoolean(isTimeout)) ++
          causedBy ++ initTime ++ waitTime ++ binding
      })
  }

}
