package com.workflows.components.transformers

import java.io.PrintWriter

import com.utils.SparkIOUtil
import com.utils.Utils.trimUtil
import com.utils.exception.ExceptionHandler.BatchException
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{input_file_name, lit, split}
import com.utils.{SparkIOUtil, StringConstantsUtil => strUtil}
import org.apache.log4j.Logger
import org.apache.spark.storage.StorageLevel

class CustomerRiskRatingsTransformer  extends TransformTrait {
  override def transform(paramsMap: Map[String, Any],
                         dataFrameMap: Map[String, DataFrame]):Map[String, DataFrame] =
  {

    try {
      val logger = SparkIOUtil.log
      logger.info("Customer Risk Ratings Transformer Started")

      val log = Logger.getLogger(this.getClass.getName)

      /* Limit structure (more overdrafts, more credit limits)
        Balances (negative or low average balance)
        KYC ratings
        Deposits and loans (more loans)
        Loan or overdrafts repayments (failed/delay to repay)
        Industry they belong to (risky industries such as wood-based industries)
        Country of origin/business (risk countries like Libya, Haiti and Chad)
        */

      val custOverdraftandcreditlimits_df = dataFrameMap.get(strUtil.CUSTOMER_OVERDRAFTS_CREDITLIMITS_DF).getOrElse(SparkIOUtil.spark.emptyDataFrame).persist(StorageLevel.MEMORY_AND_DISK)
      val custAcctbalances_df = dataFrameMap.get(strUtil.CUSTOMER_ACCOUNT_BALANCES_DF).getOrElse(SparkIOUtil.spark.emptyDataFrame).persist(StorageLevel.MEMORY_AND_DISK)
      val custKYC_df =  dataFrameMap.get(strUtil.CUSTOMER_KYC_RATINGS_DF).getOrElse(SparkIOUtil.spark.emptyDataFrame).persist(StorageLevel.MEMORY_AND_DISK)
      val custDepositsandLoans_df= dataFrameMap.get(strUtil.CUSTOMER_DEPOSITS_LOANS_DF).getOrElse(SparkIOUtil.spark.emptyDataFrame).persist(StorageLevel.MEMORY_AND_DISK)
      val custLoanrepayment_df = dataFrameMap.get(strUtil.CUSTOMER_LOAN_REPAYMENTS_DF).getOrElse(SparkIOUtil.spark.emptyDataFrame).persist(StorageLevel.MEMORY_AND_DISK)
      val custIndustry_df = dataFrameMap.get(strUtil.CUSTOMER_INDUSTRY_OF_BUSINESS_DF).getOrElse(SparkIOUtil.spark.emptyDataFrame).persist(StorageLevel.MEMORY_AND_DISK)
      val custDetails_df = dataFrameMap.get(strUtil.CUSTOMER_DETAILS_DF).getOrElse(SparkIOUtil.spark.emptyDataFrame).persist(StorageLevel.MEMORY_AND_DISK)



      val inputPath: String = paramsMap(strUtil.INPUT_FILE_PATH).toString  + strUtil.CUSTOMER_ACCOUNT_BALANCES

      logger.info(s"Input file path $inputPath")

      val samsQuestionDF = SparkIOUtil.readCSV(inputPath, true, ",",format = "csv")
        .withColumn("file_date",lit(split(split(input_file_name(),"Sams_Club_US_DailyFeed_AdditionalInformation_").getItem(1),".csv").getItem(0)))

      val samsQuestionTrimDF = trimUtil(samsQuestionDF)
      Map(strUtil.CUSTOMER_RATING_TRANSFORMER_DF -> samsQuestionTrimDF)

    }catch {
      case e: Exception => {
        val errors = new java.io.StringWriter()
        e.printStackTrace()
        e.printStackTrace(new PrintWriter(errors))
        throw new BatchException(
          "Exception in the Customer Account Balance Extractor  " + e.getMessage())
      }
    }
  }

}

