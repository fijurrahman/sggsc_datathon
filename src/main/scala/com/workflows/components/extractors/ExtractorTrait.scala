package com.workflows.components.extractors

import org.apache.spark.sql.DataFrame


/**
 * Interface for Extractor
 * Paramsmap - map of input parameters
 * dataframemap - map of dataframe objects created in the ETL
 * libraryobjects - map of <Enum,objects> of every library or service enabled through
 * enableservices parameter
 */

/*trait ExtractorTrait {

  val dataframes:Map[String, DataFrame] = Map()

  def extract(paramsMap: Map[String,Any],
              extractedDF : Option[Map[String,DataFrame]],
              libraryObjects:Option[Map[LibraryEnum, Any]]): Option[Map[String,DataFrame]]

}*/
trait ExtractorTrait {

  val dataframes:Map[String, DataFrame] = Map()

  def extract(paramsMap: Map[String,Any],
              extractedDF : Option[Map[String,DataFrame]]): Option[Map[String,DataFrame]]

}
