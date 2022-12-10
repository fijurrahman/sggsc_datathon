package com.workflows.workflow

import com.workflows.components.extractors.CustomerDetailsExtractor

object CustomerRiskRatingsWF extends WorkFlowTrait{
  addExtractors(new CustomerDetailsExtractor)

}
