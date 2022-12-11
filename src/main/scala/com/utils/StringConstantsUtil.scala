package com.utils

import java.text.SimpleDateFormat
import java.util.Calendar

object StringConstantsUtil {

  val dateformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  val WORKFLOW = "workflow"

  val CUSTOMER_RISK_RATINGS_WF ="CustomerRiskRatingsWF"

  val GLOBAL = "global"
  val MASTER = "master"
  val LOCAL = "local"
  val RUNMODE = "runMode"


  val CUSTOMERDETAILS_FEED = "input_feed_customer"
  val INPUT_LOAN_DETAILS = "loan_details"
  val CUST_ACCOUNT_BALANCES_LOD_FEED = "loan_lod_feed"
  val CUST_LOB ="customer_lob"

  val CUSTOMER_OVERDRAFTS_CREDITLIMITS = ""
  val CUSTOMER_ACCOUNT_BALANCES=""
  val CUSTOMER_DEPOSITS_LOANS =""
  val CUSTOMER_DETAILS= "src/main/resources/input/customer_details.csv"
  val CUSTOMER_KYC_RATINGS=""
  val CUSTOMER_LOAN_REPAYMENTS=""
  val CUSTOMER_INDUSTRY_OF_BUSINESS=""


  // OutDF one stage to another stage

  val CUSTOMER_OVERDRAFTS_CREDITLIMITS_DF = ""
  val CUSTOMER_ACCOUNT_BALANCES_DF = "custRunningAccountStatusDf"
  val CUSTOMER_DEPOSITS_LOANS_DF = ""
  val CUSTOMER_DETAILS_DF = "customerRefinedDf"
  val CUSTOMER_KYC_RATINGS_DF = ""
  val CUSTOMER_LOAN_REPAYMENTS_DF = ""
  val CUSTOMER_INDUSTRY_OF_BUSINESS_DF = "customerIndustryDetails"

  val CUSTOMER_RATING_TRANSFORMER_DF = "customerRiskRatingsrptDf"



  val CUST_ID = "ID"
  val CODE_GENDER = "CODE_GENDER"
  val FLAG_OWN_CAR ="FLAG_OWN_CAR"
  val FLAG_OWN_REALTY = "FLAG_OWN_REALTY"
  val CNT_CHILDREN = "CNT_CHILDREN"
  val AMT_INCOME_TOTAL ="AMT_INCOME_TOTAL"
  val NAME_INCOME_TYPE ="NAME_INCOME_TYPE"
  val NAME_EDUCATION_TYPE ="NAME_EDUCATION_TYPE"
  val NAME_FAMILY_STATUS ="NAME_FAMILY_STATUS"
  val NAME_HOUSING_TYPE = "NAME_HOUSING_TYPE"
  val AGE_IN_DAYS = "AGE_IN_DAYS"
  val AGE_IN_YEARS ="AGE_IN_YEARS"
  val DAYS_EMPLOYED = "DAYS_EMPLOYED"
  val FLAG_MOBIL = "FLAG_MOBIL"
  val FLAG_WORK_PHONE = "FLAG_WORK_PHONE"
  val FLAG_PHONE  = "FLAG_PHONE"
  val FLAG_EMAIL = "FLAG_EMAIL"
  val OCCUPATION_TYPE = "OCCUPATION_TYPE"
  val CNT_FAM_MEMBERS = "CNT_FAM_MEMBERS"

  val upd_ts = dateformat.format(Calendar.getInstance().getTime())



}
