package com.workflows.components.extractors

import java.io.PrintWriter

import com.utils.SparkIOUtil
import com.utils.Utils.trimUtil
import com.utils.exception.ExceptionHandler.BatchException
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{input_file_name, lit, split}
import com.utils.{SparkIOUtil, StringConstantsUtil => strUtil}

class CustomerDepositsLoansExtractors  extends ExtractorTrait {
  override def extract(paramsMap: Map[String, Any],
                       extractedDF: Option[Map[String, DataFrame]]):Option[Map[String,DataFrame]] =
  {

    try {
      val logger = SparkIOUtil.log
      logger.info("Deposits and loans (more loans)")

      val inputPath: String = paramsMap(strUtil.INPUT_FILE_PATH).toString  + strUtil.CUSTOMER_DEPOSITS_LOANS

      logger.info(s"Input file path $inputPath")

      val samsQuestionDF = SparkIOUtil.readCSV(inputPath, true, ",",format = "csv")
        .withColumn("file_date",lit(split(split(input_file_name(),"Sams_Club_US_DailyFeed_AdditionalInformation_").getItem(1),".csv").getItem(0)))

      val samsQuestionTrimDF = trimUtil(samsQuestionDF)
      Some(Map(strUtil.CUSTOMER_DEPOSITS_LOANS_DF -> samsQuestionTrimDF))

    }catch {
      case e: Exception => {
        val errors = new java.io.StringWriter()
        e.printStackTrace()
        e.printStackTrace(new PrintWriter(errors))
        throw new BatchException(
          "error at sams question raw Extractor  " + e.getMessage())
      }
    }
  }
}
