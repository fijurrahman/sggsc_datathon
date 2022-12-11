package com.workflows.workflow

import com.workflows.components.extractors.{CustomerAccountBalancesExtractor, CustomerDetailsExtractor, CustomerIndustryExtractor}
import com.workflows.components.loaders.CustomerRiskRatingsLoader
import com.workflows.components.transformers.CustomerRiskRatingsTransformer

object CustomerRiskRatingsWF extends WorkFlowTrait{
  addExtractors(new CustomerDetailsExtractor,new CustomerAccountBalancesExtractor , new CustomerIndustryExtractor)
  addTransformers(new CustomerRiskRatingsTransformer)
  addLoaders(new CustomerRiskRatingsLoader)

}
