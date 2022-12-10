package com.utils

import java.io.{ByteArrayOutputStream, ObjectOutputStream}

import org.apache.log4j.Logger

import scala.collection.mutable.LinkedHashSet
import org.slf4j.LoggerFactory
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, ltrim, rtrim, trim}
import org.joda.time.DateTime
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.util.matching.Regex


object Utils {


  def print(obj: Any, log: Logger): Unit = {

    val builder: StringBuilder = new StringBuilder

    obj match {
      case set: LinkedHashSet[Any] => for (obj <- set) {
        builder.append(obj.getClass.getName + "\n")
      }
        log.info(builder.toString())
    }

  }

  def printString(obj: Any): String = {

    val builder: StringBuilder = new StringBuilder

    obj match {
      case set: LinkedHashSet[Any] => for (obj <- set) {
        builder.append("\n\t" + obj.getClass.getName)
      }
    }
    builder.toString()
  }

  /**
   * This returns the approximate optimal partition number of the dataframe to repartition
   * This is useful to repartition the data frame before persisting to disk, specially for non partitioned tables .
   * This is helpful in to avoid small file issues while creating the tables
   * @param dataFrame the dataframe to be repartitioned
   * @param cacheDf Boolean field defaulted to true. Could be set to false in case of the data frame to cache is too big to fit into memory
   * @param compressionFactor Approximated compression factor for orc file with Snappy compression. Could be changed in case of precision
   * @param partitionSize the default HDFS block size of 128 MB. Could be changed in case of different requirements
   */
  def getNumberOfPartitionsToPersist(dataFrame: DataFrame,
                                     cacheDf: Boolean = true,
                                     compressionFactor: Int = 100,
                                     partitionSize: Long = 128000000L): Int = {

    val log = LoggerFactory.getLogger(this.getClass.getName)
    val df = if(cacheDf) dataFrame.cache() else dataFrame
    val rowCount = df.count()
    log.info(s"APP_INFO: Total rows count is $rowCount")
    if(rowCount > 0){
      val rowSize = getBytes(df.head).toFloat
      log.info(s"APP_INFO: Each row size is $rowSize bytes")

      val partitions = ((rowSize * rowCount.toFloat) / (partitionSize * compressionFactor)).ceil.toInt
      log.info(s"APP_INFO: Number of partitions are $partitions")
      partitions
    } else 1

  }

  def trimUtil(df: DataFrame): DataFrame = {
    df.columns.foldLeft(df) { (df, colName) =>
      df.withColumn(colName, rtrim(ltrim(trim(col(colName)))))
    }
  }

  /**
   * This returns the size in bytes for the object.
   * @param value the object to be evaluated for the size
   */
  def getBytes(value: Any): Long = {
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(stream)
    oos.writeObject(value)
    oos.close()
    stream.toByteArray.length
  }


}
