package com.components.utils

import com.utils.{StringConstantsUtil => SC}
import org.apache.spark.sql.types._

object TestSchema {

    //ID,CODE_GENDER,FLAG_OWN_CAR,FLAG_OWN_REALTY,CNT_CHILDREN,AMT_INCOME_TOTAL,NAME_INCOME_TYPE,NAME_EDUCATION_TYPE,NAME_FAMILY_STATUS,NAME_HOUSING_TYPE,AGE_IN_DAYS,AGE_IN_YEARS,DAYS_EMPLOYED,FLAG_MOBIL,FLAG_WORK_PHONE,FLAG_PHONE,FLAG_EMAIL,OCCUPATION_TYPE,CNT_FAM_MEMBERS

  val customerDetailsSchema = StructType(List(
    StructField(SC.CUST_ID,StringType,true),
    StructField(SC.CODE_GENDER,StringType,true),
    StructField(SC.FLAG_OWN_CAR,StringType,true),
    StructField(SC.FLAG_OWN_REALTY,StringType,true),
    StructField(SC.CNT_CHILDREN,StringType,true),
    StructField(SC.AMT_INCOME_TOTAL,StringType,true),
    StructField(SC.NAME_INCOME_TYPE,StringType,true),
    StructField(SC.NAME_EDUCATION_TYPE,StringType,true),
    StructField(SC.NAME_FAMILY_STATUS,StringType,true),
    StructField(SC.NAME_HOUSING_TYPE,StringType,true),
    StructField(SC.AGE_IN_DAYS,StringType,true),
    StructField(SC.AGE_IN_YEARS,StringType,true),
    StructField(SC.DAYS_EMPLOYED,StringType,true),
    StructField(SC.FLAG_MOBIL,StringType,true),
    StructField(SC.FLAG_WORK_PHONE,StringType,true),
    StructField(SC.FLAG_PHONE,StringType,true),
    StructField(SC.FLAG_EMAIL,StringType,true),
    StructField(SC.OCCUPATION_TYPE,StringType,true),
    StructField(SC.CNT_FAM_MEMBERS,StringType,true)
  ))






}
