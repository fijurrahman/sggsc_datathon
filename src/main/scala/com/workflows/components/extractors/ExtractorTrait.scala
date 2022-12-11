package com.workflows.components.extractors

import org.apache.spark.sql.DataFrame


/**
 * Interface for Extractor
 * Paramsmap - map of input parameters
 * dataframemap - map of dataframe objects created in the ETL

 */

trait ExtractorTrait {

  val dataframes:Map[String, DataFrame] = Map()

  def extract(paramsMap: Map[String,Any],
              extractedDF : Option[Map[String,DataFrame]]): Option[Map[String,DataFrame]]

}
