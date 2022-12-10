package com.utils

import java.io.PrintWriter
import java.text.SimpleDateFormat
import java.time._
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

  def createPrecatTables(df: DataFrame,format: String,schema: String,objectName: String,PartitionsColumns: Seq[String]): Unit = {
    spark.sql(s"DROP TABLE IF EXISTS ${schema}.${objectName} PURGE")
    df.write
      .format("orc")
      .partitionBy(PartitionsColumns: _*)
      .saveAsTable(schema + "." + objectName)
    log.info(s"Pre Catalog Table created sucessfully ${schema}.${objectName}" )
  }


  def createOrReplaceSummaryTable(df: DataFrame,
                                  format: String,
                                  schemaName: String,
                                  tableName: String,
                                  partitionColumns: Seq[String]= Seq("None"),datasource: String=null) : Unit = {

    try {

      var tblname = tableName

      tblname = if (datasource.equals("SAMS"))
        tableName+"_SAMS"
      else if (datasource.equals("WM"))
        tableName+"_WM"
      else if (datasource.equals("color"))
        tableName+"_color"
      else tableName+"_INTL"
      println(spark.catalog.tableExists(schemaName + "." + tblname) + "Table Status")
      if (spark.catalog.tableExists(schemaName + "." + tblname)) {
        spark.sql(s"DROP TABLE IF EXISTS ${schemaName}.${tblname}_BKP PURGE")
        spark.sql(s"DROP TABLE IF EXISTS ${schemaName}.${tblname} PURGE")
      }

      df.write.format("orc").saveAsTable(schemaName + "." + tblname)

      log.info("Table Created Sucessfully " + schemaName + "."+ tblname)

    } catch {
      case e: Exception => {
        val errors = new java.io.StringWriter()
        e.printStackTrace()
        e.printStackTrace(new PrintWriter(errors))
        throw new BatchException(
          s"error Writing Table ${tableName}  " + e.getMessage())
      }
    }

  }

  /**
   * This gracefully loads GCS bucket from the data frame.
   * Creates a backup file with existing table data.The table downtime for accessing
   * would be negligible
   * @param dataFrame the dataframe to be created as table
   * @param table table name to be created. should be the path of the refreshing table/file.
   * @param bucket Bucket details on which the table has to be created.
   * @param paramsMap To use any key/values that has been passed to the application
   * @param updateState Overwrite or append.
   * @param repartition Whether To repartition the df based on appox. file size.
   */
  def UpdateGCSBucketGracefully(dataFrame: DataFrame, table: String, bucket: String,
                                paramsMap: Map[String, Any], updateState: String , repartition : Boolean = false
                               ): Unit = {

    val log = LoggerFactory.getLogger(this.getClass.getName)
    //val path = new Path(s"gs://${paramsMap(bucket)}")
    val path = new Path(bucket)
    val fs: FileSystem = path.getFileSystem(spark.sparkContext.hadoopConfiguration)
    val filepath = bucket + table
    val originalFilePath = new Path(filepath)
    val backupPathFile = bucket + table + "_backup"
    val inActiveFile = bucket + table + "_inactive"
    val backupPath = new Path(backupPathFile)
    val inactiveFiePath = new Path(inActiveFile)

    if (fs.exists(backupPath) && fs.getFileStatus(backupPath).isDirectory) fs.delete(backupPath,true)
    if (fs.exists(inactiveFiePath) && fs.getFileStatus(inactiveFiePath).isDirectory) {
      fs.delete(inactiveFiePath,true)
      log.info(s"APP_INFO: Creating ${inactiveFiePath}")
      fs.create(inactiveFiePath)
    }

    if (!fs.exists(inactiveFiePath)) fs.create(inactiveFiePath)

    log.info(s"APP_INFO: Creating the table: $table in back up $updateState ")

    //LoadGCSBucketWithOptimumFileSize(dataFrame, s"${paramsMap(bucket)}$table".concat("_inactive"),updateState, repartition)
    LoadGCSBucketWithOptimumFileSize(dataFrame, inActiveFile,updateState, repartition)

    try{
      log.info(s"APP_INFO: Creating the back up data : $backupPath")
      if (fs.exists(originalFilePath)) fs.rename(originalFilePath,backupPath)
    } catch {
      case exception: Exception =>
        log.error(exception.getMessage)
    }
    try{
      log.info(s"APP_INFO: Creating the original target data : $originalFilePath")
      fs.rename(inactiveFiePath,originalFilePath)
    } catch {
      case exception: Exception =>
        log.error(exception.getMessage)
    }
    try{
      log.info(s"APP_INFO: Deleting the back up data : $backupPath")
      if (fs.exists(backupPath) && fs.getFileStatus(backupPath).isDirectory) fs.delete(backupPath,true)
    } catch {
      case exception: Exception =>
        log.error(exception.getMessage)
    }
  }

  /**
   * This creates a Snappy compressed ORC table from the data frame.
   * @param dataFrame the dataframe to be created as table
   * @param bucketFolderName bucket folder name to be created.
   * @param updateMode Overwrite or Append.
   * @param repartition Whether to repartition or not.
   */
  def LoadGCSBucketWithOptimumFileSize(dataFrame: DataFrame, bucketFolderName: String
                                       , updateMode: String, repartition : Boolean = false
                                      ): Unit = {

    var repartitionedDf = dataFrame
    if (repartition) {
      log.info(s"APP_INFO: Repartitioning the dataframe for persisting")
      repartitionedDf = dataFrame.repartition(Utils.getNumberOfPartitionsToPersist(dataFrame))
    }
    log.info(s"APP_INFO: Creating the table $bucketFolderName")
    if (updateMode.equalsIgnoreCase("overwrite_mode"))
      repartitionedDf.write.mode(SaveMode.Overwrite).format("orc")
        .save(s"$bucketFolderName")
    else
      repartitionedDf.write.mode(SaveMode.Append).format("orc")
        .insertInto(s"$bucketFolderName")
    log.info(s"APP_INFO: Table $bucketFolderName successfully created")
  }
  def readOrcFromGCSBucket(gcsBucket : String, table : String): DataFrame = {
    spark.read.orc(s"$gcsBucket/$table")
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

    // TODO Assign a more appropriate naming for the filesystem path
    val inaOutputPath = outputPath.concat("_ina")
    val bkpOutputPath = outputPath.concat("_bkp")

    // Delete the INA path if already exists
    fs.delete(new Path(inaOutputPath), true)

    // Persist the dataframe
    df.write.mode(SaveMode.Overwrite).format(format).save(inaOutputPath)

    // Delete the BKP location
    fs.delete(new Path(bkpOutputPath), true)

    // Alter output path to bkp
    // TODO Handle for output location not already exists
    fs.rename(path, new Path(bkpOutputPath))

    // Alter ina path to output path
    fs.rename(new Path(inaOutputPath), path)
  }






  def deDup(f: (DataFrame, String, Seq[Column], String) => DataFrame,
            df: DataFrame,
            cols: String,
            colm: Seq[Column],
            aliasName: String): DataFrame = {
    f(df, cols, colm, aliasName)
  }

  def maxFileDates(df: DataFrame,
                   cols: String,
                   colm: Seq[Column],
                   aliasName: String): DataFrame = {
    df.groupBy(colm: _*).agg(max(df.col(cols)).as(aliasName))
  }

  def minFileDates(df: DataFrame,
                   cols: String,
                   colm: Seq[Column],
                   aliasName: String): DataFrame = {
    df.groupBy(colm: _*).agg(min(df.col(cols)).as(aliasName))
  }

  def maxFileDate(df: DataFrame,
                  cols: String,
                  col: Seq[Column],
                  aliasName: String): DataFrame =
    deDup(maxFileDates, df, cols, col, aliasName)

  def minFileDate(df: DataFrame,
                  cols: String,
                  col: Seq[Column],
                  aliasName: String): DataFrame =
    deDup(minFileDates, df, cols, col, aliasName)



  def doRefreshlkptable(df: DataFrame,obectName: String) = {
    if(spark.catalog.tableExists(obectName)) {
      spark.sql(s"DROP TABLE IF EXISTS ${obectName}_BKP PURGE")
      spark.sql(s" CREATE TABLE ${obectName}_BKP AS SELECT * FROM ${obectName}")
    }
    df.write.mode(SaveMode.Overwrite).insertInto(obectName)
  }


  def convert2Upper(df: DataFrame) : DataFrame = {
    df.columns.foldLeft(df) { (df, colName) =>
      df.withColumn(colName, upper(rtrim(ltrim(trim(col(colName))))))
    }
  }

  def initCapUtil(df: DataFrame,colname: Seq[String]): DataFrame = {
    df.columns.foldLeft(df) { (df, colName) =>
      df.withColumn(colName, initcap(col(colName)))
    }.toDF(colname: _*)
  }

  def getLatestRecord (Df: DataFrame,columns: List[String]): DataFrame = {
    val windowFun = Window.partitionBy(columns.head,columns.tail :_*).orderBy(columns.head)
    val withFilterDf = Df.withColumn("rownum",row_number().over(windowFun))
    withFilterDf.filter(col("rownum") === 1).drop(col("rownum"))
  }

}
