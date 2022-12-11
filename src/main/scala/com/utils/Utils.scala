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
