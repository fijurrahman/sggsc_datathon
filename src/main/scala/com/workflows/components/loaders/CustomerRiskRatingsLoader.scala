package com.workflows.components.loaders

import com.utils.{SparkIOUtil, StringConstantsUtil => strUtil}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SaveMode}

class CustomerRiskRatingsLoader extends LoaderTrait {
  override def load(paramsMap: Map[String, Any], dataFrameMap: Map[String, DataFrame]): Unit = {
    val log = Logger.getLogger(this.getClass.getName)
    log.info("loading following DFs")

    val catlogDf = dataFrameMap.get(strUtil.CUSTOMER_RATING_TRANSFORMER_DF).get
    val outputPath: String = paramsMap(strUtil.OUTPUT_PATH).toString

    SparkIOUtil.overwriteDFToDisk(catlogDf,"csv",outputPath)

  }
}
