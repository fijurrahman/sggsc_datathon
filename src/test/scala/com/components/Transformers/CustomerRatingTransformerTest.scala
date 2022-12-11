package com.components.Transformers

import com.components.utils.SharedSparkSession
import com.components.utils.{SharedSparkSession, TestSchema => TT, TestUtils => TU}
import org.apache.spark.sql.DataFrame
import com.utils.{SparkIOUtil, StringConstantsUtil => strUtil}
import com.workflows.components.transformers.CustomerRiskRatingsTransformer

class CustomerRatingTransformerTest extends SharedSparkSession {

  var CustomerloanLOD: DataFrame = TU.createDataFrameFromCSVFile(s"/input/customer_loan_lod.csv")
  var customerDetails: DataFrame = TU.createDataFrameFromCSVFile(s"/input/customer_loan_details.csv")
  var customerIndustry: DataFrame = TU.createDataFrameFromCSVFile(s"/input/customer_lob.csv")
  var applicationCustomer: DataFrame = TU.createDataFrameFromCSVFile(s"/input/customer_details.csv")

  var expectedDf: DataFrame = TU.createDataFrameFromCSVFile(s"/expected/expectedRating.csv")

  val paramsMap: Map[String, Any] = Map.empty

  val customerLoanAndAccountBalancesDf = CustomerloanLOD.join(customerDetails
    , CustomerloanLOD.col("id") === customerDetails.col("id"),"leftouter")
    .select(customerDetails.col("ID"),
      customerDetails.col("IS_SB_ACCOUNT"),
      customerDetails.col("IS_OD_ACCOUNT"),
      customerDetails.col("IS_RD_ACCOUNT"),
      customerDetails.col("FLAG_NEGATIVE_BALANCE"),
      customerDetails.col("FLAG_PERSONAL_LOAN"),
      customerDetails.col("FLAG_LOAN_DEFAULT"),
      CustomerloanLOD.col("FD_AMT"),
      CustomerloanLOD.col("RD_BALANCE"),
      CustomerloanLOD.col("SB_CURRENT_BALANCE"),
      CustomerloanLOD.col("SB_QUARTELY_BALANCE"),
      CustomerloanLOD.col("OD_CURRENT_OUTSTANDING"),
      CustomerloanLOD.col("OD_LIMIT"),
      CustomerloanLOD.col("PERSONAL_LOAN_BALANCE")
    )



  "Transformer" should "match data with sample output" in {


    val dataFrameMap: Map[String, DataFrame] = Map(
      strUtil.CUSTOMER_ACCOUNT_BALANCES_DF -> customerLoanAndAccountBalancesDf,
      strUtil.CUSTOMER_INDUSTRY_OF_BUSINESS_DF -> customerIndustry,
      strUtil.CUSTOMER_DETAILS_DF -> applicationCustomer
    )

    val outMap: Map[String, DataFrame] = new CustomerRiskRatingsTransformer().transform(paramsMap,dataFrameMap)

    val outDf : DataFrame = outMap(strUtil.CUSTOMER_RATING_TRANSFORMER_DF)
    outDf.show()
    expectedDf.printSchema()
    assert(expectedDf.count() == outDf.count())

  }




}
