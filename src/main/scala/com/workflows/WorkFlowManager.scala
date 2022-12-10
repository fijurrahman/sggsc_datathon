package com.workflows

import com.utils.{Utils, StringConstantsUtil => StrConst}
import com.workflows.workflow.{CustomerRiskRatingsWF, WorkFlowTrait}
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

/**
 * Manages all workflows. Invokes workflow based on the input parameter 'workflow' and co-ordinates the invocation
 * of all components in the workflow. Abstracts argument passing and invocation.
 * Maintains the order of components as declared in the Implementation class
 */

object WorkFlowManager {

  val log = Logger.getLogger(this.getClass.getName)

  def manageWorkFlow(paramsMap:Map[String, Any]) : Unit = {

   val workflowInstance:Option[WorkFlowTrait] =  paramsMap.get(StrConst.WORKFLOW) match {

      case Some(StrConst.CUSTOMER_RISK_RATINGS_WF) => {
                        log.debug("Invoking " + StrConst.CUSTOMER_RISK_RATINGS_WF)
                        Some(CustomerRiskRatingsWF)
      }

      case Some(x) => log.error("No Workflow implementation available for " + x)
                      None
    }

    workflowInstance match {

        case Some(x) => executeFlow(paramsMap, workflowInstance.get)

        case None => log.error("No Workflow implementation available")
    }

  }

  def executeFlow(paramsMap:Map[String, Any], workflow:WorkFlowTrait) : Unit = {

    var resultantDFMap : Map[String,DataFrame] = Map[String,DataFrame]()

    val extractorsSet = workflow.extractorsSet
    val transformersSet = workflow.transformersSet
    val loadersSet = workflow.loadersSet

    val printString = Utils.printString(extractorsSet) + Utils.printString(transformersSet) +
      Utils.printString(loadersSet)

    log.info("Running " + paramsMap.get(StrConst.WORKFLOW) + " having " + printString)

    extractorsSet.map {
      ext => {
        val result = ext.extract(paramsMap, Some(resultantDFMap))
        result match {
          case None =>
          case map:Option[Map[String,DataFrame]] => resultantDFMap = resultantDFMap ++ map.get
        }
      }
    }

    transformersSet.map {

      trans => {
        val result = trans.transform(paramsMap, resultantDFMap)
        resultantDFMap = resultantDFMap ++ result
      }
    }


    loadersSet.map{

      loader => {
        loader.load(paramsMap, resultantDFMap)
      }
    }
  }





}
