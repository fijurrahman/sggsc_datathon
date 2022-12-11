package com.workflows.components.extractors

import java.io.PrintWriter

import com.utils.Utils.trimUtil
import com.utils.exception.ExceptionHandler.BatchException
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{input_file_name, lit, split}
import com.utils.{SparkIOUtil, StringConstantsUtil => strUtil}

class CustomerAccountBalancesExtractor  extends ExtractorTrait {
  override def extract(paramsMap: Map[String, Any],
                       extractedDF: Option[Map[String, DataFrame]]):Option[Map[String,DataFrame]] =
  {

    try {
      val logger = SparkIOUtil.log
      logger.info("Customer Account Balances and Loan Details and Default Customer Extractor")

      val inputPathForLoanDetails: String = paramsMap(strUtil.CUST_ACCOUNT_BALANCES_LOD_FEED).toString
      val inputPathForLOD: String = paramsMap(strUtil.INPUT_LOAN_DETAILS).toString

      logger.info(s"Input file path $inputPathForLoanDetails and $inputPathForLOD")

      val custAccountLoanDetails = SparkIOUtil.readCSV(inputPathForLoanDetails, true, ",",format = "csv")

      val custAccountLoanLOD = SparkIOUtil.readCSV(inputPathForLOD, true, ",",format = "csv")

      val customerLoanAndAccountBalancesDf = custAccountLoanDetails.join(custAccountLoanLOD
        , custAccountLoanDetails.col("id") === custAccountLoanLOD.col("id"),"leftouter")
        .select(custAccountLoanLOD.col("ID"),
          custAccountLoanLOD.col("IS_SB_ACCOUNT"),
          custAccountLoanLOD.col("IS_OD_ACCOUNT"),
          custAccountLoanLOD.col("IS_RD_ACCOUNT"),
          custAccountLoanLOD.col("FLAG_NEGATIVE_BALANCE"),
          custAccountLoanLOD.col("FLAG_PERSONAL_LOAN"),
          custAccountLoanLOD.col("FLAG_LOAN_DEFAULT"),
          custAccountLoanDetails.col("FD_AMT"),
          custAccountLoanDetails.col("RD_BALANCE"),
          custAccountLoanDetails.col("SB_CURRENT_BALANCE"),
          custAccountLoanDetails.col("SB_QUARTELY_BALANCE"),
          custAccountLoanDetails.col("OD_CURRENT_OUTSTANDING"),
          custAccountLoanDetails.col("OD_LIMIT"),
          custAccountLoanDetails.col("PERSONAL_LOAN_BALANCE")
        )

      val custRunningAccountStatusDf = trimUtil(customerLoanAndAccountBalancesDf).withColumn("upd_ts", lit(strUtil.upd_ts))


      Some(Map(strUtil.CUSTOMER_ACCOUNT_BALANCES_DF -> custRunningAccountStatusDf))

    }catch {
      case e: Exception => {
        val errors = new java.io.StringWriter()
        e.printStackTrace()
        e.printStackTrace(new PrintWriter(errors))
        throw new BatchException(
          "Exception in the Customer Account Balance and Loan Details Extractor  " + e.getMessage())
      }
    }
  }
}