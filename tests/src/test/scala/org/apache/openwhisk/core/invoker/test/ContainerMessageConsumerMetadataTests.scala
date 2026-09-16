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

package org.apache.openwhisk.core.invoker.test

import org.apache.openwhisk.core.entity.Attachments.Inline
import org.apache.openwhisk.core.entity.ExecManifest.{ImageName, RuntimeManifest}
import org.apache.openwhisk.core.entity._
import org.apache.openwhisk.core.entity.size._
import org.apache.openwhisk.core.invoker.ContainerMessageConsumer
import org.junit.runner.RunWith
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ContainerMessageConsumerMetadataTests extends AnyFlatSpec with Matchers {
  it should "build reusable-concurrency container metadata without worker action code fetch" in {
    val manifest = RuntimeManifest("reusable-concurrency:1", ImageName("runtime"))
    val revision = DocRevision("7-exact")
    val metadata = WhiskActionMetaData(
      EntityPath("namespace"),
      EntityName("protected"),
      CodeExecMetaDataAsString(manifest, binary = true, entryPoint = Some("main")),
      limits = ActionLimits(memory = MemoryLimit(256.MB)))

    val action = ContainerMessageConsumer.metadataOnlyAction(metadata, revision).toOption.get
    val actionExec = action.exec.asInstanceOf[CodeExecAsAttachment]
    action.rev shouldBe revision
    actionExec.kind shouldBe "reusable-concurrency:1"
    actionExec.binary shouldBe true
    actionExec.code shouldBe Inline("")
    action.limits shouldBe metadata.limits
  }
}
