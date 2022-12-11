package com.workflows.workflow

import com.workflows.components.extractors.{CustomerAccountBalancesExtractor, CustomerDetailsExtractor, CustomerIndustryExtractor}

object CustomerRiskRatingsWF extends WorkFlowTrait{
  addExtractors(new CustomerDetailsExtractor,new CustomerAccountBalancesExtractor , new CustomerIndustryExtractor)

}
