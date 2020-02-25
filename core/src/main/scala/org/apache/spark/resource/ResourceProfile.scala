/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.resource

import java.util.{Map => JMap}
import java.util.concurrent.atomic.AtomicInteger
import javax.annotation.concurrent.GuardedBy

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.annotation.Evolving
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.internal.config.Python.PYSPARK_EXECUTOR_MEMORY
import org.apache.spark.util.Utils

/**
 * Resource profile to associate with an RDD. A ResourceProfile allows the user to
 * specify executor and task requirements for an RDD that will get applied during a
 * stage. This allows the user to change the resource requirements between stages.
 * This is meant to be immutable so user can't change it after building.
 */
@Evolving
class ResourceProfile(
    val executorResources: Map[String, ExecutorResourceRequest],
    val taskResources: Map[String, TaskResourceRequest]) extends Serializable with Logging {

  // _id is only a var for testing purposes
  private var _id = ResourceProfile.getNextProfileId
  // This is used for any resources that use fractional amounts, the key is the resource name
  // and the value is the number of tasks that can share a resource address. For example,
  // if the user says task gpu amount is 0.5, that results in 2 tasks per resource address.
  private var _executorResourceSlotsPerAddr: Option[Map[String, Int]] = None
  private var _limitingResource: Option[String] = None
  private var _maxTasksPerExecutor: Option[Int] = None
  private var _coresLimitKnown: Boolean = false

  def id: Int = _id

  /**
   * (Java-specific) gets a Java Map of resources to TaskResourceRequest
   */
  def taskResourcesJMap: JMap[String, TaskResourceRequest] = taskResources.asJava

  /**
   * (Java-specific) gets a Java Map of resources to ExecutorResourceRequest
   */
  def executorResourcesJMap: JMap[String, ExecutorResourceRequest] = {
    executorResources.asJava
  }

  // Note that some cluster managers don't set the executor cores explicitly so
  // be sure to check the Option as required
  private[spark] def getExecutorCores: Option[Int] = {
    executorResources.get(ResourceProfile.CORES).map(_.amount.toInt)
  }

  private[spark] def getTaskCpus: Option[Int] = {
    taskResources.get(ResourceProfile.CPUS).map(_.amount.toInt)
  }

  private[spark] def getNumSlotsPerAddress(resource: String, sparkConf: SparkConf): Int = {
    _executorResourceSlotsPerAddr.getOrElse {
      calculateTasksAndLimitingResource(sparkConf)
    }
    _executorResourceSlotsPerAddr.get.getOrElse(resource,
      throw new SparkException(s"Resource $resource doesn't exist in profile id: $id"))
  }

  // Maximum tasks you could put on an executor with this profile based on the limiting resource.
  // If the executor cores config is not present this value is based on the other resources
  // available or 1 if no other resources. You need to check the isCoresLimitKnown to
  // calculate proper value.
  private[spark] def maxTasksPerExecutor(sparkConf: SparkConf): Int = {
    _maxTasksPerExecutor.getOrElse {
      calculateTasksAndLimitingResource(sparkConf)
      _maxTasksPerExecutor.get
    }
  }

  // Returns whether the executor cores was available to use to calculate the max tasks
  // per executor and limiting resource. Some cluster managers (like standalone and coarse
  // grained mesos) don't use the cores config by default so we can't use it to calculate slots.
  private[spark] def isCoresLimitKnown: Boolean = _coresLimitKnown

  // The resource that has the least amount of slots per executor. Its possible multiple or all
  // resources result in same number of slots and this could be any of those.
  // If the executor cores config is not present this value is based on the other resources
  // available or empty string if no other resources. You need to check the isCoresLimitKnown to
  // calculate proper value.
  private[spark] def limitingResource(sparkConf: SparkConf): String = {
    _limitingResource.getOrElse {
      calculateTasksAndLimitingResource(sparkConf)
      _limitingResource.get
    }
  }

  // executor cores config is not set for some masters by default and the default value
  // only applies to yarn/k8s
  private def shouldCheckExecutorCores(sparkConf: SparkConf): Boolean = {
    val master = sparkConf.getOption("spark.master")
    sparkConf.contains(EXECUTOR_CORES) ||
      (master.isDefined && (master.get.equalsIgnoreCase("yarn") || master.get.startsWith("k8s")))
  }

  /**
   * Utility function to calculate the number of tasks you can run on a single Executor based
   * on the task and executor resource requests in the ResourceProfile. This will be based
   * off the resource that is most restrictive. For instance, if the executor
   * request is for 4 cpus and 2 gpus and your task request is for 1 cpu and 1 gpu each, the
   * limiting resource is gpu and the number of tasks you can run on a single executor is 2.
   * This function also sets the limiting resource, isCoresLimitKnown and number of slots per
   * resource address.
   */
  private def calculateTasksAndLimitingResource(sparkConf: SparkConf): Unit = synchronized {
    val shouldCheckExecCores = shouldCheckExecutorCores(sparkConf)
    var (taskLimit, limitingResource) = if (shouldCheckExecCores) {
      val cpusPerTask = taskResources.get(ResourceProfile.CPUS)
        .map(_.amount).getOrElse(sparkConf.get(CPUS_PER_TASK).toDouble).toInt
      assert(cpusPerTask > 0, "CPUs per task configuration has to be > 0")
      val coresPerExecutor = getExecutorCores.getOrElse(sparkConf.get(EXECUTOR_CORES))
      _coresLimitKnown = true
      ResourceUtils.validateTaskCpusLargeEnough(coresPerExecutor, cpusPerTask)
      val tasksBasedOnCores = coresPerExecutor / cpusPerTask
      // Note that if the cores per executor aren't set properly this calculation could be off,
      // we default it to just be 1 in order to allow checking of the rest of the custom
      // resources. We set the limit based on the other resources available.
      (tasksBasedOnCores, ResourceProfile.CPUS)
    } else {
      (-1, "")
    }
    val numPartsPerResourceMap = new mutable.HashMap[String, Int]
    numPartsPerResourceMap(ResourceProfile.CORES) = 1
    val taskResourcesToCheck = new mutable.HashMap[String, TaskResourceRequest]
    taskResourcesToCheck ++= ResourceProfile.getCustomTaskResources(this)
    val execResourceToCheck = ResourceProfile.getCustomExecutorResources(this)
    execResourceToCheck.foreach { case (rName, execReq) =>
      val taskReq = taskResources.get(rName).map(_.amount).getOrElse(0.0)
      numPartsPerResourceMap(rName) = 1
      if (taskReq > 0.0) {
        if (taskReq > execReq.amount) {
          throw new SparkException(s"The executor resource: $rName, amount: ${execReq.amount} " +
            s"needs to be >= the task resource request amount of $taskReq")
        }
        val (numPerTask, parts) = ResourceUtils.calculateAmountAndPartsForFraction(taskReq)
        numPartsPerResourceMap(rName) = parts
        val numTasks = ((execReq.amount * parts) / numPerTask).toInt
        if (taskLimit == -1 || numTasks < taskLimit) {
          if (shouldCheckExecCores) {
            // TODO - until resource profiles full implemented we need to error if cores not
            // limiting resource because the scheduler code uses that for slots
            throw new IllegalArgumentException("The number of slots on an executor has to be " +
              "limited by the number of cores, otherwise you waste resources and " +
              "dynamic allocation doesn't work properly. Your configuration has " +
              s"core/task cpu slots = ${taskLimit} and " +
              s"${execReq.resourceName} = ${numTasks}. " +
              "Please adjust your configuration so that all resources require same number " +
              "of executor slots.")
          }
          limitingResource = rName
          taskLimit = numTasks
        }
        taskResourcesToCheck -= rName
      } else {
        logWarning(s"The executor resource config for resource: $rName was specified but " +
          "no corresponding task resource request was specified.")
      }
    }
    if(!shouldCheckExecCores && Utils.isDynamicAllocationEnabled(sparkConf)) {
      // if we can't rely on the executor cores config throw a warning for user
      logWarning("Please ensure that the number of slots available on your " +
        "executors is limited by the number of cores to task cpus and not another " +
        "custom resource. If cores is not the limiting resource then dynamic " +
        "allocation will not work properly!")
    }
    if (taskResourcesToCheck.nonEmpty) {
      throw new SparkException("No executor resource configs were not specified for the " +
        s"following task configs: ${taskResourcesToCheck.keys.mkString(",")}")
    }
    if (limitingResource.nonEmpty) {
      logInfo(s"Limiting resource is $limitingResource at $taskLimit tasks per executor")
    }
    _executorResourceSlotsPerAddr = Some(numPartsPerResourceMap.toMap)
    _maxTasksPerExecutor = if (taskLimit == -1) Some(1) else Some(taskLimit)
    _limitingResource = Some(limitingResource)
    if (shouldCheckExecCores) {
      ResourceUtils.warnOnWastedResources(this, sparkConf)
    }
  }

  // to be used only by history server for reconstruction from events
  private[spark] def setResourceProfileId(id: Int): Unit = {
    _id = id
  }

  // testing only
  private[spark] def setToDefaultProfile(): Unit = {
    _id = ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case that: ResourceProfile =>
        that.getClass == this.getClass && that.id == _id &&
          that.taskResources == taskResources && that.executorResources == executorResources
      case _ =>
        false
    }
  }

  override def hashCode(): Int = Seq(taskResources, executorResources).hashCode()

  override def toString(): String = {
    s"Profile: id = ${_id}, executor resources: ${executorResources.mkString(",")}, " +
      s"task resources: ${taskResources.mkString(",")}"
  }
}

object ResourceProfile extends Logging {
  // task resources
  val CPUS = "cpus"
  // Executor resources
  val CORES = "cores"
  val MEMORY = "memory"
  val OVERHEAD_MEM = "memoryOverhead"
  val PYSPARK_MEM = "pyspark.memory"

  // all supported spark executor resources (minus the custom resources like GPUs/FPGAs)
  val allSupportedExecutorResources = Seq(CORES, MEMORY, OVERHEAD_MEM, PYSPARK_MEM)

  val UNKNOWN_RESOURCE_PROFILE_ID = -1
  val DEFAULT_RESOURCE_PROFILE_ID = 0

  private lazy val nextProfileId = new AtomicInteger(0)
  private val DEFAULT_PROFILE_LOCK = new Object()

  // The default resource profile uses the application level configs.
  // var so that it can be reset for testing purposes.
  @GuardedBy("DEFAULT_PROFILE_LOCK")
  private var defaultProfile: Option[ResourceProfile] = None

  private[spark] def getNextProfileId: Int = nextProfileId.getAndIncrement()

  private[spark] def getOrCreateDefaultProfile(conf: SparkConf): ResourceProfile = {
    DEFAULT_PROFILE_LOCK.synchronized {
      defaultProfile match {
        case Some(prof) => prof
        case None =>
          val taskResources = getDefaultTaskResources(conf)
          val executorResources = getDefaultExecutorResources(conf)
          val defProf = new ResourceProfile(executorResources, taskResources)
          defProf.setToDefaultProfile()
          defaultProfile = Some(defProf)
          logInfo("Default ResourceProfile created, executor resources: " +
            s"${defProf.executorResources}, task resources: " +
            s"${defProf.taskResources}")
          defProf
      }
    }
  }

  private def getDefaultTaskResources(conf: SparkConf): Map[String, TaskResourceRequest] = {
    val cpusPerTask = conf.get(CPUS_PER_TASK)
    val treqs = new TaskResourceRequests().cpus(cpusPerTask)
    ResourceUtils.addTaskResourceRequests(conf, treqs)
    treqs.requests
  }

  private def getDefaultExecutorResources(conf: SparkConf): Map[String, ExecutorResourceRequest] = {
    val ereqs = new ExecutorResourceRequests()
    ereqs.cores(conf.get(EXECUTOR_CORES))
    ereqs.memory(conf.get(EXECUTOR_MEMORY).toString)
    conf.get(EXECUTOR_MEMORY_OVERHEAD).map(mem => ereqs.memoryOverhead(mem.toString))
    conf.get(PYSPARK_EXECUTOR_MEMORY).map(mem => ereqs.pysparkMemory(mem.toString))
    val execReq = ResourceUtils.parseAllResourceRequests(conf, SPARK_EXECUTOR_PREFIX)
    execReq.foreach { req =>
      val name = req.id.resourceName
      ereqs.resource(name, req.amount, req.discoveryScript.orElse(""),
        req.vendor.orElse(""))
    }
    ereqs.requests
  }

  // for testing only
  private[spark] def reInitDefaultProfile(conf: SparkConf): Unit = {
    clearDefaultProfile()
    // force recreate it after clearing
    getOrCreateDefaultProfile(conf)
  }

  private[spark] def clearDefaultProfile(): Unit = {
    DEFAULT_PROFILE_LOCK.synchronized {
      defaultProfile = None
    }
  }

  private[spark] def getCustomTaskResources(
      rp: ResourceProfile): Map[String, TaskResourceRequest] = {
    rp.taskResources.filterKeys(k => !k.equals(ResourceProfile.CPUS))
  }

  private[spark] def getCustomExecutorResources(
      rp: ResourceProfile): Map[String, ExecutorResourceRequest] = {
    rp.executorResources.filterKeys(k => !ResourceProfile.allSupportedExecutorResources.contains(k))
  }
}
