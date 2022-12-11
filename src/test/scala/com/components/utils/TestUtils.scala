package com.components.utils

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object TestUtils {
  lazy val spark: SparkSession = {
    SparkSession.builder()
      .master("local")
      .appName("spark test")
      .config("spark.sql.shuffle.partitions", "1")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .enableHiveSupport()
      .getOrCreate()

  }

  def getSchemaForTable(tableName: String): StructType = {
    spark.sql(s"select * from $tableName limit 1").schema
  }

  def readCSV(csvFilePath: String, schema: StructType): DataFrame = {
    var df : DataFrame = null
    if(schema != null) {
      df = spark.read.format("csv"). // Use "csv" regardless of TSV or CSV.
        option("header", "true").
        option("delimiter", ",").
        schema(schema).
        load(csvFilePath)
    } else{
      df = spark.read.format("csv"). // Use "csv" regardless of TSV or CSV.
        option("header", "true").
        option("delimiter", ",").
        option("inferschema","true").
        load(csvFilePath)
    }
    df
  }

  def createEmptyDataFrame(schema: StructType): DataFrame = {
    spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
  }

  def createDataFrameFromCSVFile(filePath: String): DataFrame = {
    val file = getClass.getResource(filePath).getPath
    TestUtils.readCSV(file, null)
  }

  def createDataFrameFromCSV(schema: StructType, filePath: String): DataFrame = {
    val file = getClass.getResource(filePath).getPath
    TestUtils.readCSV(file, schema)
  }

  def createDataFrameFromCSVFile(schema: StructType, filePath: String): DataFrame = {
    val file = getClass.getResource(filePath).getPath
    readCSV(file, schema)
  }


}
