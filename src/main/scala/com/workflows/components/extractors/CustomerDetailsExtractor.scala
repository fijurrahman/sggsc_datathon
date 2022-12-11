package com.workflows.components.extractors

import java.io.PrintWriter


import com.utils.Utils.trimUtil
import com.utils.exception.ExceptionHandler.BatchException
import org.apache.spark.sql.DataFrame
import com.utils.{SparkIOUtil, StringConstantsUtil => strUtil}

class CustomerDetailsExtractor  extends ExtractorTrait {
  override def extract(paramsMap: Map[String, Any],
                       extractedDF: Option[Map[String, DataFrame]]):Option[Map[String,DataFrame]] =
  {

    try {
      val logger = SparkIOUtil.log
      logger.info("Customer Details Feed extraction Process Started")

      val inputPath: String = paramsMap(strUtil.CUSTOMERDETAILS_FEED).toString

      logger.info(s"Input file path $inputPath")

      val customerDf = SparkIOUtil.readCSV(inputPath, true, ",",format = "csv")

      val customerRefinedDf = trimUtil(customerDf)

      Some(Map(strUtil.CUSTOMER_DETAILS_DF -> customerRefinedDf))

    }catch {
      case e: Exception => {
        val errors = new java.io.StringWriter()
        e.printStackTrace()
        e.printStackTrace(new PrintWriter(errors))
        throw new BatchException(
          "Customer Details Extraction Error  " + e.getMessage())
      }
    }
  }
}
