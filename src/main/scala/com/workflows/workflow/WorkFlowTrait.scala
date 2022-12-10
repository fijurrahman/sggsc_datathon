package com.workflows.workflow

import com.workflows.components.extractors.ExtractorTrait
import com.workflows.components.loaders.LoaderTrait
import com.workflows.components.transformers.TransformTrait
import org.apache.log4j.Logger

import scala.collection.mutable.LinkedHashSet

/**
 * Backbone for Workflow. Maintains all workflow components.
 */
trait WorkFlowTrait {

  val log = Logger.getLogger(this.getClass.getName)

  val extractorsSet : LinkedHashSet[ExtractorTrait] = LinkedHashSet[ExtractorTrait]()
  val transformersSet : LinkedHashSet[TransformTrait] = LinkedHashSet[TransformTrait]()
  val loadersSet : LinkedHashSet[LoaderTrait] = LinkedHashSet[LoaderTrait]()

  def addExtractors(extractors: ExtractorTrait *):Unit = {

    for (ext <- extractors) {
      extractorsSet.add(ext)
    }

  }

  def addTransformers(transformers: TransformTrait *):Unit = {

    for (tran <- transformers) {
      transformersSet.add(tran)
    }

  }

  def addLoaders(loaders: LoaderTrait *):Unit = {

    for (load <- loaders) {
      loadersSet.add(load)
    }

  }

}
