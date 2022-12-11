package com.utils

import com.typesafe.config.{Config, ConfigObject, ConfigValue}
import com.typesafe.scalalogging.StrictLogging

import java.util.Properties
import scala.collection.JavaConverters._
import scala.io.Source

/**
 * Update the paramMap object by loading the appConf details.
 */
object WorkflowRunnerConfigureUtil extends StrictLogging {




  /**
   * Update paramMap without parsing and updating any of the path values.
   *
   * @param currentParamMap currentParamMap
   * @param appConfig       appConfig
   * @return
   */
  def updateParamMap(currentParamMap: Map[String, Any], appConfig: Config): Map[String, Any] = {
    logger.info("Loading the paramMap with appConf details.")
    var newParamMap: Map[String, Any] = currentParamMap

    // Load the default values in the newParamMap first
    appConfig.getObject("default").forEach(new java.util.function.BiConsumer[String, ConfigValue]() {
      def accept(key: String, value: ConfigValue): Unit = {
        newParamMap += (key -> value.unwrapped().toString)
      }
    })

    // Load env specific properties
    // Default to "local"
    if (!currentParamMap.exists(_._1 == "env")) {
      logger.warn("`env` flag value not found in paramMap, hence defaulting to `local`")
    }

    appConfig.getObject(currentParamMap.getOrElse("env", "local").toString).forEach(new java.util.function.BiConsumer[String, ConfigValue]() {
      def accept(key: String, value: ConfigValue): Unit = {
        newParamMap += (key -> value.unwrapped().toString)
      }
    })

    logger.info("paramMap after processing " + newParamMap.toList.toString())
    newParamMap
  }

   /**
   * Builds map of key,value pair using config object
   *
   * @param confObj confObj
   * @return
   */
  def loadMap(confObj: ConfigObject): Map[String, AnyRef] = {

    val itr = confObj.unwrapped().entrySet().iterator()

    val confMap: scala.collection.mutable.Map[String, AnyRef] = scala.collection.mutable.Map[String, AnyRef]()

    while (itr.hasNext) {
      val e = itr.next()
      confMap.put(e.getKey, e.getValue)
    }
    confMap.toMap
  }

}
