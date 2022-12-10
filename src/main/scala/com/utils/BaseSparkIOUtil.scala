package com.utils

import com.utils.{BaseStringConstants => BC}
import org.apache.spark.sql._
import org.slf4j.{Logger, LoggerFactory}



class BaseSparkIOUtil {

  val log: Logger = LoggerFactory.getLogger(classOf[BaseSparkIOUtil])

  var spark: SparkSession = _

  /**
   *
   * @param configMap
   */
  def configureSpark(configMap: Map[String, Any]): Unit = {

    val appName = configMap(BC.WORKFLOW).toString
    val master = configMap(BC.MASTER).toString

    log.debug("configMap.get(BC.RUNTYPE)" + configMap.get(BC.RUNTYPE))

    val sparkSession = configMap.get(BC.RUNTYPE) match {

      case Some(x: String) if (x.equalsIgnoreCase(BC.STANDALONE)) => SparkSession.builder().appName(appName).master(master)

      case None => SparkSession.builder().appName(appName).master(master)

      case Some(x: String) => log.error("Illegal argument Exception - no matching RUNTYPE for " + x)
        throw new IllegalArgumentException("Illegal argument Exception - no matching RUNTYPE for " + x)
    }

    master match {
      case BC.LOCAL =>
      case _ => sparkSession.enableHiveSupport()
    }

    for ((key, value) <- configMap.-(BC.WORKFLOW).-(BC.MASTER)) {

      value match {
        case bool: Boolean =>
          sparkSession.config(key, bool)
        case str: String =>
          sparkSession.config(key, str)
        case d: Double =>
          sparkSession.config(key, d)
        case l: Long =>
          sparkSession.config(key, l)
        case i: Int =>
          sparkSession.config(key, i)
        case _ =>
      }
    }

    BaseSparkIOUtil.sparkSession = sparkSession.getOrCreate()
    spark = BaseSparkIOUtil.sparkSession

    val arr = getAllSparkConfig

    log.debug("Sparksession configured with following parameters ")

    arr.foreach(each => log.debug(each._1 + "=" + each._2))
  }

  def loadArgument(key: String): String = {
    val value = spark.sparkContext.getConf.get(key, System.getProperty(key))

    log.info("For key " + key + " found value " + value)
    value;

  }


  def getAllSparkConfig: Array[(String, String)] = {
    spark.sparkContext.getConf.getAll
  }

  def getSparkSession: SparkSession = spark


  def execute(sql: String): Unit = {
    log.debug("Executing SQL = " + sql)
    spark.sql(sql).collect()
  }

  /*def writeOrc(df: DataFrame, mode: SaveMode, tableName: String, partition: Option[String]): Unit = {
    partition match {
      case Some(x) => df.write.mode(mode).format("orc").partitionBy(x).saveAsTable(tableName)

      case None => df.write.mode(mode).format("orc").saveAsTable(tableName)
    }
  }

  def insertOrc(df: DataFrame, mode: SaveMode, tableName: String, partition: Option[String]): Unit = {
    partition match {
      case Some(x) => df.write.mode(mode).format("orc").partitionBy(x).insertInto(tableName)

      case None => df.write.mode(mode).format("orc").insertInto(tableName)
    }
  }

  def writeOrc(df: DataFrame, mode: SaveMode, tableName: String, partition: List[String]): Unit = {
    df.write.mode(mode).format("orc").partitionBy(partition: _*).saveAsTable(tableName)
  }

  def insertOrc(df: DataFrame, mode: SaveMode, tableName: String, partition: List[String]): Unit = {
    df.write.mode(mode).format("orc").partitionBy(partition: _*).insertInto(tableName)
  }*/

  def fetch(sql: String): DataFrame = {
    log.debug("Executing SQL = " + sql)
    spark.sql(sql)
  }

  def fetch(sourceConfig: String, sql: String): DataFrame = {
    log.debug("Executing SQL = " + sql)

    val sourceProperties = ConfigUtil.getSourceProperties(sourceConfig);

    spark.read.jdbc(url = sourceProperties.get("url").toString, table = sql, sourceProperties)
  }

  def isTableExists(database: String, tableName: String): Boolean = {
    spark.catalog.tableExists(database + "." + tableName)
  }

  def refreshMetaData(schema: String, table: String): Unit = {
    spark.catalog.refreshTable(schema + "." + table)
  }


  def buildTempDF(): DataFrame = {
    val rdd = spark.sparkContext.parallelize(List(1, 2, 3))
    import SparkImplicits._
    rdd.toDF()
  }

  def stopContext(): Unit = {
    if (!spark.sparkContext.isStopped)
      spark.stop
  }

}

object BaseSparkIOUtil {
  var sparkSession: SparkSession = _

  def setPool(poolName: String): Unit = {
    sparkSession.sparkContext.setLocalProperty("spark.scheduler.pool", poolName)
  }

  def getSparkJobDetails: Map[String, String] = {
    Map(BC.APP_ID -> sparkSession.sparkContext.applicationId,
      BC.APP_USER -> sparkSession.sparkContext.sparkUser)
  }
}

object SparkImplicits extends SQLImplicits with Serializable {
  protected override def _sqlContext: SQLContext = BaseSparkIOUtil.sparkSession.sqlContext
}
