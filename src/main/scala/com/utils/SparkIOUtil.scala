package com.utils


import java.time.format._

import com.utils.{BaseSparkIOUtil, Utils}
import com.utils.exception.ExceptionHandler.BatchException
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, SaveMode}
import org.slf4j.LoggerFactory


object SparkIOUtil extends BaseSparkIOUtil {

  def print(): Unit = {
    println(spark.getClass)
  }


  def checkSchema(actualSchema: StructType, expectedSchema: StructType): Boolean = {
    // Set actualSchema fields as nullable=true by default
    val actualSchemaUpd: Seq[StructField] = StructType(actualSchema.sortBy(_.name)).map(_.copy(nullable = true))

    // Set expectedSchema fields as nullable=true by default
    val expectedSchemaUpd: Seq[StructField] = StructType(expectedSchema.sortBy(_.name)).map(_.copy(nullable = true))

    actualSchemaUpd == expectedSchemaUpd
  }


  def parseStaticDateTimeFormat(inputDateTime: String): String = {
    val inputDateTimeFormat = "yyyyMMddHHmm"
    // input_format = 202102142352
    val inputDateTimeFormatter = DateTimeFormatter.ofPattern(inputDateTimeFormat)

    val outputDateTimeFormat = "yyyy-MM-dd HH:mm:ss.SSS"
    //output_format = yyyy-MM-dd HH:mm:ss.SSS
    val outputDateTimeFormatter = DateTimeFormatter.ofPattern(outputDateTimeFormat)

    log.info(s"parsing datetime [${inputDateTime.padTo(12, '0')}]")

    outputDateTimeFormatter.format(inputDateTimeFormatter.parse(inputDateTime.padTo(12, '0')))
  }



  def readCSV(file: String,
              hasHeader: Boolean = false,
              sep: String,
              quoteChar: String = null,format: String=null): DataFrame = {
    if ((hasHeader)) {
      if (quoteChar == null && format.equalsIgnoreCase("csv")) {
        spark.read
          .option("sep", sep)
          .option("inferSchema", value = true)
          .option("header", value = true)
          .csv(file)
      } else if (format.equalsIgnoreCase("csv")) {
        spark.read
          .option("sep", sep)
          .option("quote", quoteChar)
          .option("inferSchema", value = true)
          .option("header", value = true)
          .csv(file)
      } else {
        spark.read
          //.option("sep", sep)
          //.option("quote", quoteChar)
          //.option("inferSchema", value = true)
          //.option("header", value = true)
          .orc(file)
      }
    } else {
      spark.read.format("csv").load(file)
    }
  }


  /**
   * Write dataframe  local file system.
   */
  def overwriteDFToDisk(df: DataFrame, format: String, outputPath: String): Unit = {
    val path: Path = new Path(outputPath)
    val fs: FileSystem = path.getFileSystem(spark.sparkContext.hadoopConfiguration)

    // Delete alternate location if already exists
    // or throw error
    //    val hadoopfs: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)


    val inaOutputPath = outputPath.concat("_ina")
    val bkpOutputPath = outputPath.concat("_bkp")

    // Delete the INA path if already exists
    fs.delete(new Path(inaOutputPath), true)

    // Persist the dataframe
    df.write.option("header",true).mode(SaveMode.Overwrite).format(format).save(inaOutputPath)

    // Delete the BKP location
    fs.delete(new Path(bkpOutputPath), true)

    // Alter output path to bkp
    // TODO Handle for output location not already exists
    fs.rename(path, new Path(bkpOutputPath))

    // Alter ina path to output path
    fs.rename(new Path(inaOutputPath), path)
  }

}
