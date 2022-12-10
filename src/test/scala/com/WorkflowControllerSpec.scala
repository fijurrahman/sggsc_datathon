package com

import com.utils.ConfigUtil
import com.utils.WorkflowRunnerConfigureUtil
import com.workflows.WorkflowController


/**
 * Test class for Workflow Controller.
 */
class WorkflowControllerSpec extends BaseSpec  {


  "Running Workflow controller in local env" should "generate the output in console" taggedAs LocalOnlyTests in {
    logger.info("Running workflow controller with dev ENV params")


    paramsMap = Map("runtype" -> "standalone", "runmode" -> "local", "env" -> "local", "workflow" -> "CustomerRiskRatingsWF")
    paramsMap = WorkflowRunnerConfigureUtil.updateParamMap(paramsMap, ConfigUtil.appConfig)

    val args: Array[String] = Array("runtype=standalone", "runmode=local", "env=local", "workflow=CustomerRiskRatingsWF")

    WorkflowController.main(args)

  }



  "Providing no workflow name" should "result in no execution" in {
    logger.info("Running WorkflowController with test ENV params and no workflow name")

    val args: Array[String] = Array("runtype=standalone", "runmode=local", "env=local")

    assertThrows[NoSuchElementException] {
      WorkflowController.main(args)
    }
  }



}
