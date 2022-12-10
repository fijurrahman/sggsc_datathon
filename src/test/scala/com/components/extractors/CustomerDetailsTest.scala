package com.components.extractors

import com.LocalOnlyTests
import com.components.utils.{SharedSparkSession, TestSchema => TT, TestUtils => TU}
import org.apache.spark.sql.{DataFrame, Row}
import com.utils.{WorkflowRunnerConfigureUtil, StringConstantsUtil => strUtil}
import com.workflows.components.extractors.CustomerDetailsExtractor
import com.typesafe.config.{Config, ConfigFactory}


class CustomerDetailsTest extends SharedSparkSession {

  //Prepare data for transformer
  var rawDf: DataFrame = TU.createDataFrameFromCSVFile(TT.customerDetailsSchema, s"/input/application_record.csv")
  var expTransDf: DataFrame = TU.createDataFrameFromCSVFile(TT.customerDetailsSchema, s"/input/application_record.csv")

  val appConfig: Config = ConfigFactory.parseResources("appConf.conf")

  val paramMap: Map[String, Any] = Map("runtype" -> "standalone", "runmode" -> "local", "env" -> "local", "workflow" -> "CustomerRiskRatingsDatapipeline")

  //Prepare args for transformer
  val updatedParamMap: Map[String, Any] = WorkflowRunnerConfigureUtil.updateParamMap(paramMap, appConfig)


  val dataFrameMap : Option[Map[String, DataFrame]] = Option(Map(
    strUtil.CUSTOMER_DETAILS_DF -> rawDf
  ))

  //Assertion
  "Transformer" should "fail in case of no dataframe " in {

    assertThrows[com.utils.exception.ExceptionHandler.BatchException]( new CustomerDetailsExtractor().extract(
      updatedParamMap
      ,Option(Map.empty)
   ))
  }


  "Transformer" should "return empty dataframe in case of empty dataframe input" in  {
    val inputmap: Map[String,Any] = Map("input_feed" -> "src/main/resources/input/application_record_empty.csv")

    val deptEmptyDf = spark.createDataFrame(spark.sparkContext.emptyRDD[Row],TT.customerDetailsSchema)
    val outMap = new CustomerDetailsExtractor().extract(
      updatedParamMap ++ inputmap
      ,Option(Map(strUtil.CUSTOMER_DETAILS_DF -> spark.createDataFrame(spark.sparkContext.emptyRDD[Row],TT.customerDetailsSchema)))
      )

    val outDf : DataFrame = outMap.get(strUtil.CUSTOMER_DETAILS_DF)

    assert(outDf.count == 0)
    assertSmallDatasetEquality(deptEmptyDf,outDf)
  }

  /*"Transformer" should "match data with sample output" in {
    val inputmap: Map[String,Any] = Map("input_feed" -> "src/main/resources/input/application_record.csv")

    val outMap: Option[Map[String, DataFrame]] = new CustomerDetailsExtractor().extract(updatedParamMap ++  inputmap,dataFrameMap)

    val outDf : DataFrame = outMap.get(strUtil.CUSTOMER_DETAILS_DF)

    assert(expTransDf.count() == outDf.count())
    assertSmallDatasetEquality(expTransDf.sort("CUST_ID"),
      outDf.sort("CUST_ID"))
  }*/

}
