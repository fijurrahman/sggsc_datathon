package com.workflows.components.transformers

import java.io.PrintWriter

import com.utils.Utils.trimUtil
import com.utils.exception.ExceptionHandler.BatchException
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{abs, col, lit, when}
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

      custAcctbalances_df.printSchema()
      custIndustry_df.printSchema()
      custDetails_df.printSchema()


      log.info("Process Started for the data set custAcctbalances_df,custIndustry_df,custDetails_df")

      val customerRiskRatingsDf = custDetails_df.join(custIndustry_df
        , custDetails_df.col("COUNTRY_CODE") === custIndustry_df.col("COUNTRY_CODE"),"leftouter")
          .select(custDetails_df.col("ID"),
            custDetails_df.col("CODE_GENDER"),
            custDetails_df.col("FLAG_OWN_CAR"),
            custDetails_df.col("FLAG_OWN_REALTY"),
            custDetails_df.col("CNT_CHILDREN"),
            custDetails_df.col("AMT_INCOME_TOTAL"),
            custDetails_df.col("NAME_INCOME_TYPE"),
            custDetails_df.col("NAME_EDUCATION_TYPE"),
            custDetails_df.col("NAME_FAMILY_STATUS"),
            custDetails_df.col("NAME_HOUSING_TYPE"),
            custDetails_df.col("AGE_IN_DAYS"),
            custDetails_df.col("AGE_IN_YEARS"),
            custDetails_df.col("DAYS_EMPLOYED"),
            custDetails_df.col("FLAG_MOBIL"),
            custDetails_df.col("FLAG_WORK_PHONE"),
            custDetails_df.col("FLAG_PHONE"),
            custDetails_df.col("FLAG_EMAIL"),
            custDetails_df.col("OCCUPATION_TYPE"),
            custDetails_df.col("CNT_FAM_MEMBERS"),
            custIndustry_df.col("COUNTRY_CODE"),
            custIndustry_df.col("CUSTOMER_COUNTRY_OF_BUSINESS"),
            custIndustry_df.col("CUSTOMER_LOB"),
            custIndustry_df.col("CUSTOMER_BUSINESS_ADDRESS"),
            custIndustry_df.col("BUSINESS_TYPE"))

      log.info("customer Details with LOB")

      val customerRiskRatingsDetailsDf = customerRiskRatingsDf.join(custAcctbalances_df
        ,customerRiskRatingsDf.col("id") === custAcctbalances_df.col("id") )
        .select(custDetails_df.col("ID"),
          custDetails_df.col("CODE_GENDER"),
          custDetails_df.col("FLAG_OWN_CAR"),
          custDetails_df.col("FLAG_OWN_REALTY"),
          custDetails_df.col("CNT_CHILDREN"),
          custDetails_df.col("AMT_INCOME_TOTAL"),
          custDetails_df.col("NAME_INCOME_TYPE"),
          custDetails_df.col("NAME_EDUCATION_TYPE"),
          custDetails_df.col("NAME_FAMILY_STATUS"),
          custDetails_df.col("NAME_HOUSING_TYPE"),
          custDetails_df.col("AGE_IN_DAYS"),
          custDetails_df.col("AGE_IN_YEARS"),
          custDetails_df.col("DAYS_EMPLOYED"),
          custDetails_df.col("FLAG_MOBIL"),
          custDetails_df.col("FLAG_WORK_PHONE"),
          custDetails_df.col("FLAG_PHONE"),
          custDetails_df.col("FLAG_EMAIL"),
          custDetails_df.col("OCCUPATION_TYPE"),
          custDetails_df.col("CNT_FAM_MEMBERS"),
          custIndustry_df.col("COUNTRY_CODE"),
          custIndustry_df.col("CUSTOMER_COUNTRY_OF_BUSINESS"),
          custIndustry_df.col("CUSTOMER_LOB"),
          custIndustry_df.col("CUSTOMER_BUSINESS_ADDRESS"),
          custIndustry_df.col("BUSINESS_TYPE"),
          custAcctbalances_df.col("IS_SB_ACCOUNT"),
          custAcctbalances_df.col("IS_OD_ACCOUNT"),
          custAcctbalances_df.col("IS_RD_ACCOUNT"),
          custAcctbalances_df.col("FLAG_NEGATIVE_BALANCE"),
          custAcctbalances_df.col("FLAG_PERSONAL_LOAN"),
          custAcctbalances_df.col("FLAG_LOAN_DEFAULT"),
          custAcctbalances_df.col("FD_AMT"),
          custAcctbalances_df.col("RD_BALANCE"),
          custAcctbalances_df.col("SB_CURRENT_BALANCE"),
          custAcctbalances_df.col("SB_QUARTELY_BALANCE"),
          custAcctbalances_df.col("OD_CURRENT_OUTSTANDING"),
          custAcctbalances_df.col("OD_LIMIT"),
          custAcctbalances_df.col("PERSONAL_LOAN_BALANCE"))


      log.info("Customer RiskRatings Df")
      customerRiskRatingsDetailsDf.printSchema()

      val crrrating = customerRiskRatingsDetailsDf.
        withColumn("ratings",when(col("FLAG_LOAN_DEFAULT") === "Y","High")
          .when(col("CUSTOMER_COUNTRY_OF_BUSINESS").isin(strUtil.riskCountry: _*),"High")
          .when(abs(col("AGE_IN_YEARS"))> strUtil.moreAge && col("FLAG_LOAN_DEFAULT") =!= 'Y',"Medium")
          .when( (col("FD_AMT") + col("SB_CURRENT_BALANCE")  + col("RD_BALANCE")) -
            (col("PERSONAL_LOAN_BALANCE") + col("OD_CURRENT_OUTSTANDING")) > col("PERSONAL_LOAN_BALANCE"),"Low")
            .otherwise("Low")
        ).withColumn("upd_ts", lit(strUtil.upd_ts))

      //crrrating.show()

      val customerRiskRatingsrptDf = trimUtil(crrrating)



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

