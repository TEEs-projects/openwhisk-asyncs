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

package org.apache.openwhisk.core.containerpool.v2

import java.net.{HttpURLConnection, URL, URLEncoder}
import java.nio.charset.StandardCharsets

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.pattern.after
import org.apache.openwhisk.core.containerpool.{ContainerAddress, ContainerId}
import org.apache.openwhisk.core.scheduler.queue.GatewayControlClient
import spray.json._

import scala.concurrent.blocking
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.io.Source
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

/** Opaque control-side handle for one ACTIVE container target. */
final case class TargetBinding(id: Long) {
  require(id > 0, "target binding id must be positive")
}

/** Concrete container identity and its executor binding writer. */
final case class TargetContainer(containerId: ContainerId,
                                 address: ContainerAddress,
                                 kind: String,
                                 activateBinding: (Long, FiniteDuration) => Future[Unit])

/**
 * Lifecycle seam implemented by profiles that require target-bound dispatch.
 * A provider must not complete awaitReady until its real runtime session is ACTIVE.
 */
trait TargetBindingProvider {
  def requiresBinding(kind: String): Boolean
  def awaitReady(target: TargetContainer): Future[TargetBinding]
  def closeBinding(target: TargetContainer, binding: TargetBinding): Future[Unit]
}

object TargetBindingProvider {

  val ReusableConcurrencyKind = "reusable-concurrency:1"
  val BridgeControlHostEnv = "REUSABLE_TARGET_BRIDGE_CONTROL_HOST"
  val BridgeControlPortEnv = "REUSABLE_TARGET_BRIDGE_CONTROL_PORT"

  /** Existing profiles explicitly have no target-binding lifecycle. */
  object Disabled extends TargetBindingProvider {
    override def requiresBinding(kind: String): Boolean = false

    override def awaitReady(target: TargetContainer): Future[TargetBinding] =
      Future.failed(new IllegalStateException("target binding provider is disabled"))

    override def closeBinding(target: TargetContainer, binding: TargetBinding): Future[Unit] =
      Future.failed(new IllegalStateException("target binding provider is disabled"))
  }
}

/** Worker-routable DH endpoint allocated by the bridge for one concrete container. */
final case class TargetEndpointBinding(containerIdentity: String, endpointHost: String, endpointPort: Int) {
  require(containerIdentity.nonEmpty, "target endpoint container identity must not be empty")
  require(endpointHost.nonEmpty, "target endpoint host must not be empty")
  require(endpointPort > 0 && endpointPort <= 65535, "target endpoint port is invalid")
}

/** Worker-local bridge that maps one allocated DH endpoint to one concrete container endpoint. */
trait TargetEndpointBridgeClient {
  def bind(containerIdentity: String, targetHost: String, targetPort: Int): Future[TargetEndpointBinding]
  def unbind(containerIdentity: String): Future[Unit]
}

object HttpTargetEndpointBridgeClient {
  private[v2] def parseBindingResponse(expectedContainerIdentity: String,
                                       responseBody: String): TargetEndpointBinding = {
    val response = responseBody.parseJson.asJsObject
    val binding = response.fields.get("binding") match {
      case Some(value: JsObject) => value
      case _                     => throw new IllegalArgumentException("target endpoint bridge response is missing binding")
    }

    def requiredString(field: String): String =
      binding.fields.get(field) match {
        case Some(JsString(value)) if value.trim.nonEmpty => value.trim
        case _                                            => throw new IllegalArgumentException(s"target endpoint bridge response has invalid $field")
      }

    val containerIdentity = requiredString("containerIdentity")
    if (containerIdentity != expectedContainerIdentity) {
      throw new IllegalArgumentException("target endpoint bridge response has the wrong container identity")
    }
    val endpointHost = requiredString("endpointHost")
    val endpointPort = binding.fields.get("endpointPort") match {
      case Some(JsNumber(value)) if value.isWhole && value >= 1 && value <= 65535 => value.toInt
      case _                                                                      => throw new IllegalArgumentException("target endpoint bridge response has invalid endpointPort")
    }
    TargetEndpointBinding(containerIdentity, endpointHost, endpointPort)
  }
}

final class HttpTargetEndpointBridgeClient(controlHost: String,
                                           controlPort: Int,
                                           connectTimeout: FiniteDuration,
                                           readTimeout: FiniteDuration)(implicit ec: ExecutionContext)
    extends TargetEndpointBridgeClient {

  require(controlHost.nonEmpty, "target bridge control host must not be empty")
  require(controlPort > 0 && controlPort <= 65535, "target bridge control port is invalid")

  override def bind(containerIdentity: String, targetHost: String, targetPort: Int): Future[TargetEndpointBinding] = {
    val body = JsObject(
      "containerIdentity" -> JsString(containerIdentity),
      "targetHost" -> JsString(targetHost),
      "targetPort" -> JsNumber(targetPort)).compactPrint
    request("PUT", "/binding", Some(body))
      .flatMap { responseBody =>
        Future
          .fromTry(Try(HttpTargetEndpointBridgeClient.parseBindingResponse(containerIdentity, responseBody)))
          .recoverWith {
            case NonFatal(parseFailure) =>
              unbind(containerIdentity).transformWith(_ => Future.failed(parseFailure))
          }
      }
  }

  override def unbind(containerIdentity: String): Future[Unit] =
    request("DELETE", s"/binding/${URLEncoder.encode(containerIdentity, StandardCharsets.UTF_8.name())}", None).map(_ =>
      ())

  private def request(method: String, path: String, body: Option[String]): Future[String] = Future {
    blocking {
      val connection =
        new URL(s"http://$controlHost:$controlPort$path").openConnection().asInstanceOf[HttpURLConnection]
      try {
        connection.setConnectTimeout(timeoutMillis(connectTimeout))
        connection.setReadTimeout(timeoutMillis(readTimeout))
        connection.setRequestMethod(method)
        body.foreach { value =>
          val bytes = value.getBytes(StandardCharsets.UTF_8)
          connection.setDoOutput(true)
          connection.setRequestProperty("Content-Type", "application/json")
          connection.setFixedLengthStreamingMode(bytes.length)
          val output = connection.getOutputStream
          try output.write(bytes)
          finally output.close()
        }
        val status = connection.getResponseCode
        val stream = Option(if (status / 100 == 2) connection.getInputStream else connection.getErrorStream)
        val responseBody = stream
          .map { input =>
            val source = Source.fromInputStream(input, StandardCharsets.UTF_8.name())
            try source.mkString
            finally source.close()
          }
          .getOrElse("")
        if (status / 100 != 2) {
          throw new IllegalStateException(s"target endpoint bridge $method failed: status=$status")
        }
        responseBody
      } finally connection.disconnect()
    }
  }

  private def timeoutMillis(duration: FiniteDuration): Int =
    math.min(duration.toMillis, Int.MaxValue.toLong).toInt
}

/**
 * Experiment-scoped provider backed by the real control Gateway P1 wire.
 * Registration completes only after P1 reports ACTIVE and the same concrete
 * executor accepts the opaque binding id.
 */
final class GatewayTargetBindingProvider(
  gatewayClient: GatewayControlClient,
  bridgeClient: TargetEndpointBridgeClient,
  containerEndpointPort: Int,
  readyTimeout: FiniteDuration,
  retryInterval: FiniteDuration)(implicit actorSystem: ActorSystem, ec: ExecutionContext)
    extends TargetBindingProvider {

  require(containerEndpointPort > 0 && containerEndpointPort <= 65535, "container target endpoint port is invalid")
  require(readyTimeout.toMillis > 0, "target readiness timeout must be positive")
  require(retryInterval.toMillis > 0, "target readiness retry interval must be positive")

  override def requiresBinding(kind: String): Boolean = kind == TargetBindingProvider.ReusableConcurrencyKind

  override def awaitReady(target: TargetContainer): Future[TargetBinding] = {
    require(requiresBinding(target.kind), s"target binding is not enabled for ${target.kind}")
    val deadline = readyTimeout.fromNow
    val containerIdentity = target.containerId.asString

    def register(endpoint: TargetEndpointBinding): Future[TargetBinding] =
      gatewayClient
        .registerTarget(containerIdentity, endpoint.endpointHost, endpoint.endpointPort)
        .flatMap {
          case Right(registered) =>
            target
              .activateBinding(registered.targetBindingId, readyTimeout)
              .map(_ => TargetBinding(registered.targetBindingId))
              .recoverWith {
                case activationFailure =>
                  closeGateway(registered.targetBindingId).transformWith(_ => Future.failed(activationFailure))
              }
          case Left(error) if deadline.hasTimeLeft() =>
            after(retryInterval, actorSystem.scheduler)(register(endpoint))
          case Left(error) =>
            Future.failed(new IllegalStateException(s"Gateway target registration failed: ${error.message}"))
        }

    bridgeClient
      .bind(containerIdentity, target.address.host, containerEndpointPort)
      .flatMap { endpoint =>
        register(endpoint).recoverWith {
          case failure =>
            bridgeClient.unbind(containerIdentity).transformWith(_ => Future.failed(failure))
        }
      }
  }

  override def closeBinding(target: TargetContainer, binding: TargetBinding): Future[Unit] =
    closeGateway(binding.id).transformWith {
      case Success(_) => bridgeClient.unbind(target.containerId.asString)
      case Failure(closeFailure) =>
        bridgeClient.unbind(target.containerId.asString).transformWith(_ => Future.failed(closeFailure))
    }

  private def closeGateway(targetBindingId: Long): Future[Unit] =
    gatewayClient.closeTarget(targetBindingId).flatMap {
      case Right(_)    => Future.successful(())
      case Left(error) => Future.failed(new IllegalStateException(s"Gateway target close failed: ${error.message}"))
    }
}
