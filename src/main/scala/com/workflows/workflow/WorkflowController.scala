package com.workflows.workflow

import com.utils.{ConfigUtil, SparkIOUtil, StringConstantsUtil}
import com.workflows.WorkFlowManager
import org.apache.log4j.Logger

/**
 * Controller - configures Spark, loads configurations and delegates control to
 * workflow manager
 *
 */

object WorkflowController{

    var paramsMap:Map[String, Any] = scala.collection.immutable.Map()

    val log = Logger.getLogger(this.getClass.getName)

    def configureSpark(paramsMap:Map[String, Any]):Unit = {

        val allParams = paramsMap ++ ConfigUtil.getSparkGlobalConfig(paramsMap.get(StringConstantsUtil.RUNMODE)) ++
          ConfigUtil.getSparkAppConfig(paramsMap.get(StringConstantsUtil.WORKFLOW).get.toString)

        SparkIOUtil.configureSpark(allParams);

        log.debug("Spark is configured")
    }

    def main(args: Array[String]): Unit = {

        if (args.length == 0) {

        } else{
            for (arg <- args) {
                val res = arg.split("=")
                paramsMap = paramsMap + (res(0) -> res(1))
            }
        }

        configureSpark(paramsMap)

        WorkFlowManager.manageWorkFlow(paramsMap)
    }



}
