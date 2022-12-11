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


      val custAcctbalances_df = dataFrameMap.get(strUtil.CUSTOMER_ACCOUNT_BALANCES_DF).getOrElse(SparkIOUtil.spark.emptyDataFrame).persist(StorageLevel.MEMORY_AND_DISK)
      val custIndustry_df = dataFrameMap.get(strUtil.CUSTOMER_INDUSTRY_OF_BUSINESS_DF).getOrElse(SparkIOUtil.spark.emptyDataFrame).persist(StorageLevel.MEMORY_AND_DISK)
      val custDetails_df = dataFrameMap.get(strUtil.CUSTOMER_DETAILS_DF).getOrElse(SparkIOUtil.spark.emptyDataFrame).persist(StorageLevel.MEMORY_AND_DISK)

      log.info("Process Started for the data set custAcctbalances_df,custIndustry_df,custDetails_df")

      val customerRiskRatingsDf = custDetails_df.alias("customerDetails").join(custIndustry_df
        , custDetails_df.col("COUNTRY_CODE") === custIndustry_df.col("COUNTRY_CODE"),"leftouter").select("customerBalances.*")

      log.info("customer Details with LOB")
      customerRiskRatingsDf.printSchema()
      customerRiskRatingsDf.show()

      val customerRiskRatingsDetailsDf = customerRiskRatingsDf.alias("crrDf").join(custAcctbalances_df
        ,customerRiskRatingsDf.col("id") === custAcctbalances_df.col("id") ).select("crrDf.*")

      log.info("Customer RiskRatings Df")

      customerRiskRatingsDetailsDf.printSchema()
      customerRiskRatingsDetailsDf.show()

      val customerRiskRatingsrptDf = trimUtil(customerRiskRatingsDetailsDf)

      customerRiskRatingsrptDf.show()

      Map(strUtil.CUSTOMER_RATING_TRANSFORMER_DF -> customerRiskRatingsrptDf)

    }catch {
      case e: Exception => {
        val errors = new java.io.StringWriter()
        e.printStackTrace()
        e.printStackTrace(new PrintWriter(errors))
        throw new BatchException(
          "Exception in the Customer Risk Ratings Transformer  " + e.getMessage())
      }
    }
  }

}

