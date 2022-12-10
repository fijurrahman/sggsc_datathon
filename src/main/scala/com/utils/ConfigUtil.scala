package com.utils

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory, ConfigObject}
import org.apache.log4j.Logger

import scala.collection._

object ConfigUtil {

  val sparkConfig: Config = ConfigFactory.parseResources("sparkConf.conf")
  val appConfig: Config = ConfigFactory.parseResources("appConf.conf")

  val logger = Logger.getLogger(this.getClass.getName)

  def getSparkGlobalConfig(runMode:Option[Any]) : Map[String, AnyRef] = {

    logger.debug("Running ->" + runMode.getOrElse(StringConstantsUtil.LOCAL).toString.toUpperCase())

    val confObj = sparkConfig.getObject(runMode.getOrElse(StringConstantsUtil.LOCAL).toString.toUpperCase())

    loadMap(confObj)
  }

  def getSparkAppConfig(appName:String) : Map[String, AnyRef] = {

    val confObj = if (sparkConfig.hasPath(appName))  Some(sparkConfig.getObject(appName)) else None

    confObj match {
      case Some(obj:ConfigObject) => loadMap(obj)

      case None => Map()

    }
  }

  def loadMap(confObj : ConfigObject) : Map[String, AnyRef] = {

    val itr = confObj.unwrapped().entrySet().iterator()

    val confMap: mutable.Map[String, AnyRef] = mutable.Map[String, AnyRef]()

    while (itr.hasNext) {
      val e = itr.next()
      confMap.put(e.getKey, e.getValue)
    }
    confMap.toMap
  }

  def getSourceProperties(sourceName:String) :  Properties = {

    val confObj = appConfig.getObject(sourceName)

    val itr = confObj.unwrapped().entrySet().iterator()

    val properties = new Properties();

    while (itr.hasNext) {
      val e = itr.next()
      properties.put(e.getKey, e.getValue)
    }

    properties
  }


  def getAppName(appName:String): String = sparkConfig.getString("app.appName")

  def getWarehouseLocation: String = sparkConfig.getString("app.warehousedir")


}
