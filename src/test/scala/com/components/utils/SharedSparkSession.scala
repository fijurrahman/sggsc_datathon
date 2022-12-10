package com.components.utils

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import com.utils.SparkIOUtil
import org.apache.spark.sql.SparkSession
import org.scalatest.FlatSpec

trait SharedSparkSession extends FlatSpec with DatasetComparer {

  val spark: SparkSession = TestUtils.spark

  SparkIOUtil.spark = spark


}
