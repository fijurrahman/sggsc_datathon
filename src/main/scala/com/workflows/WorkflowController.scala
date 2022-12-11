package com.workflows

import com.typesafe.scalalogging.Logger
import com.utils._

/**
 * Controller - configures Spark, loads configurations and delegates control to
 * workflow manager
 *
 */
object WorkflowController {

  val logger: Logger = Logger("WorkflowRunner")

  /**
   * Configures spark by using the configurations defined in spark-conf.conf and workflow specific
   * config from app-config.config
   *
   * @param paramsMap paramsMap
   */
  def configureSpark(paramsMap: Map[String, Any]): Unit = {
    val allParams = paramsMap ++ ConfigUtil.getSparkGlobalConfig(paramsMap.get(BaseStringConstants.RUNMODE)) ++
      ConfigUtil.getSparkAppConfig(paramsMap(BaseStringConstants.WORKFLOW).toString)

    SparkIOUtil.configureSpark(allParams)
    logger.info("APP_INFO: Spark is configured")
  }

  /**
   * @param args args
   */
  def runWorkflow(args: Array[String]): Unit = {

    var paramsMap: Map[String, Any] = scala.collection.immutable.Map()

    if (args.length == 0) {
      logger.warn("Args list is Empty")
    } else {
      for (arg <- args) {
        val res = arg.split("=")
        paramsMap = paramsMap + (res(0) -> res(1))
      }
    }

    paramsMap = WorkflowRunnerConfigureUtil.updateParamMap(paramsMap, ConfigUtil.appConfig)
    logger.info(s"APP_INFO: Updated paramsmap $paramsMap")
    configureSpark(paramsMap)
    WorkFlowManager.manageWorkFlow(paramsMap)
  }

  /**
   * Main method to be called if the WORKFLOW is invoked
   * through Spark-submit
   *
   * Program Arguments :
   * runtype=standalone
   * runmode=global
   * env=local
   * workflow=TemplateWorkflow
   *
   * @param args args
   */
  def main(args: Array[String]): Unit = {

    // Printing program arguments
    args.foreach(x => logger.info(x))

    runWorkflow(args)
  }

}
