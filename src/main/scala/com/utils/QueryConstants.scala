package com.utils

object QueryConstants {


  val itemPerf_rpt_for_busdt  =  "select mds_fam_id, all_links_item_nbr,geo_region_cd,  trim(op_cmpny_cd) as op_cmpny_cd ,  crncy_cd, sales_amt,  sales_unit_qty,  comp_sales_amt,  comp_sales_unit_qty,  net_rcpt_qty,  net_rcpt_rtl_amt,  net_rcpt_cost_amt,  ho_mumd_rtl_amt,  str_mumd_rtl_amt,  str_oh_unit_qty, bus_dt from ww_item_perf_ddcl_tables.ddcl_kpi_item_perf_rpt_dly_summ_ext " +
    "where bus_dt = '$bus_dt'and geo_region_cd='$geo_region_cd' and trim(op_cmpny_cd)='$op_cmpny_cd'"

  val item_query = "(SELECT ITEM_NBR,DEPT_NBR,FINELINE_NBR,UPC_NBR FROM US_WM_VM.ITEM sample 20000) item";

  val instock_query = "(SELECT ITEM_NBR,STORE_NBR,ONHAND_EACH_QTY FROM US_WM_VM.STORE_ITEM_INSTOCK sample 20000) instock"

}
