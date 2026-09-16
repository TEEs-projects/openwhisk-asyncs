/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0.
 */

package org.apache.openwhisk.core.containerpool.v2.test

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Promise}

import org.apache.openwhisk.core.containerpool.v2.FunctionPullingContainerProxy
import org.junit.runner.RunWith
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class C1BackendPressureNativeExecutionBoundaryTests extends AnyFlatSpec with Matchers {
  implicit private val executionContext: ExecutionContext = ExecutionContext.global

  behavior of "C1 backend-pressure native execution boundaries"

  it should "bracket a marked native container run and close only after completion" in {
    val emitted = ArrayBuffer.empty[(String, String)]
    val result = Promise[String]()

    val wrapped = FunctionPullingContainerProxy.withC1NativeBackendPressureExecutionBoundaries(
      markedBackendPressure = true,
      actionKind = "nodejs:20",
      emit = (code, boundary) => emitted += code -> boundary)(result.future)

    emitted.toSeq shouldBe Seq("N420" -> "native_action_container_run_enter")
    result.success("ok")
    Await.result(wrapped, 2.seconds) shouldBe "ok"
    emitted.toSeq shouldBe Seq(
      "N420" -> "native_action_container_run_enter",
      "N430" -> "native_action_container_run_exit")
  }

  it should "leave unmarked and non-native runs untouched" in {
    val emitted = ArrayBuffer.empty[(String, String)]

    Await.result(
      FunctionPullingContainerProxy.withC1NativeBackendPressureExecutionBoundaries(
        markedBackendPressure = false,
        actionKind = "nodejs:20",
        emit = (code, boundary) => emitted += code -> boundary)(scala.concurrent.Future.successful("unmarked")),
      2.seconds) shouldBe "unmarked"
    Await.result(
      FunctionPullingContainerProxy.withC1NativeBackendPressureExecutionBoundaries(
        markedBackendPressure = true,
        actionKind = "asyncs:1",
        emit = (code, boundary) => emitted += code -> boundary)(scala.concurrent.Future.successful("asyncs")),
      2.seconds) shouldBe "asyncs"

    emitted shouldBe empty
  }

  it should "close the interval when the container run fails" in {
    val emitted = ArrayBuffer.empty[(String, String)]
    val result = Promise[String]()
    val wrapped = FunctionPullingContainerProxy.withC1NativeBackendPressureExecutionBoundaries(
      markedBackendPressure = true,
      actionKind = "nodejs:20",
      emit = (code, boundary) => emitted += code -> boundary)(result.future)

    result.failure(new RuntimeException("fixture failure"))
    Await.ready(wrapped, 2.seconds)
    emitted.map(_._1).toSeq shouldBe Seq("N420", "N430")
  }
}
