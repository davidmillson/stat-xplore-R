### this file contains some examples of functions built to scrape particular
### datasets. The PIP and DLA ones actually use two api queries per function
### to get around the limits on the number of cells. The resulting dataframes 
### are then bound together

get_scotland_CA_entitled = function(lookback = 10) {
  database = "str:database:CA_Entitled"
  measures = "str:count:CA_Entitled:V_F_CA_Entitled"
  dimensions = c("str:field:CA_Entitled:F_CA_QTR:DATE_NAME",
                 "str:field:CA_Entitled:V_F_CA_Entitled:CCSEX",
                 "str:field:CA_Entitled:V_F_CA_Entitled:CNAGE",
                 "str:field:CA_Entitled:V_F_CA_Entitled:COA_CODE",
                 "str:field:CA_Entitled:V_F_CA_Entitled:CTDURTN",
                 "str:field:CA_Entitled:V_F_CA_Entitled:CCCLIENT")
  recodes = list(
    `str:field:CA_Entitled:V_F_CA_Entitled:COA_CODE` = list(
      map = list("str:value:CA_Entitled:V_F_CA_Entitled:COA_CODE:V_C_GEOG00_COUNTRY_to_GB:S92000003")),
    `str:field:CA_Entitled:V_F_CA_Entitled:CNAGE` = auto_recodes(
      "str:valueset:CA_Entitled:V_F_CA_Entitled:CNAGE:C_CA_SINGLE_AGE"),
    `str:field:CA_Entitled:V_F_CA_Entitled:CCCLIENT` = list(
      map = list("str:value:CA_Entitled:V_F_CA_Entitled:CCCLIENT:C_CA_CCCLIENT:2",
                 "str:value:CA_Entitled:V_F_CA_Entitled:CCCLIENT:C_CA_CCCLIENT:3")
    )
  )
  build_stat_xplore_body(database, measures, dimensions, recodes) %>%
    scrape_from_stat_xplore(lookback)
}

get_scotland_CA_in_payment = function(lookback = 10) {
  database = "str:database:CA_In_Payment"
  measures = "str:count:CA_In_Payment:V_F_CA_In_Payment"
  dimensions = c("str:field:CA_In_Payment:F_CA_QTR:DATE_NAME",
                 "str:field:CA_In_Payment:V_F_CA_In_Payment:CCSEX",
                 "str:field:CA_In_Payment:V_F_CA_In_Payment:CNAGE",
                 "str:field:CA_In_Payment:V_F_CA_In_Payment:COA_CODE",
                 "str:field:CA_In_Payment:V_F_CA_In_Payment:CTDURTN",
                 "str:field:CA_In_Payment:V_F_CA_In_Payment:CCCLIENT")
  recodes = list(
    `str:field:CA_In_Payment:V_F_CA_In_Payment:COA_CODE` = list(
      map = list("str:value:CA_In_Payment:V_F_CA_In_Payment:COA_CODE:V_C_GEOG00_COUNTRY_to_GB:S92000003")),
    `str:field:CA_In_Payment:V_F_CA_In_Payment:CNAGE` = auto_recodes(
      "str:valueset:CA_In_Payment:V_F_CA_In_Payment:CNAGE:C_CA_SINGLE_AGE"),
    `str:field:CA_In_Payment:V_F_CA_In_Payment:CCCLIENT` = list(
      map = list("str:value:CA_In_Payment:V_F_CA_In_Payment:CCCLIENT:C_CA_CCCLIENT:2",
                 "str:value:CA_In_Payment:V_F_CA_In_Payment:CCCLIENT:C_CA_CCCLIENT:3")
    )
  )
  build_stat_xplore_body(database, measures, dimensions, recodes) %>%
    scrape_from_stat_xplore(lookback)
}

get_scotland_CA_amounts = function(lookback = 10) {
  database = "str:database:CA_In_Payment"
  measures = "str:statfn:CA_In_Payment:V_F_CA_In_Payment:CAWKLYAMT:MEAN"
  dimensions = c("str:field:CA_In_Payment:F_CA_QTR:DATE_NAME",
                 "str:field:CA_In_Payment:V_F_CA_In_Payment:CCSEX",
                 "str:field:CA_In_Payment:V_F_CA_In_Payment:CNAGE",
                 "str:field:CA_In_Payment:V_F_CA_In_Payment:COA_CODE",
                 "str:field:CA_In_Payment:V_F_CA_In_Payment:CTDURTN",
                 "str:field:CA_In_Payment:V_F_CA_In_Payment:CCCLIENT")
  recodes = list(
    `str:field:CA_In_Payment:V_F_CA_In_Payment:COA_CODE` = list(
      map = list("str:value:CA_In_Payment:V_F_CA_In_Payment:COA_CODE:V_C_GEOG00_COUNTRY_to_GB:S92000003")),
    `str:field:CA_In_Payment:V_F_CA_In_Payment:CNAGE` = auto_recodes(
      "str:valueset:CA_In_Payment:V_F_CA_In_Payment:CNAGE:C_CA_SINGLE_AGE"),
    `str:field:CA_In_Payment:V_F_CA_In_Payment:CCCLIENT` = list(
      map = list("str:value:CA_In_Payment:V_F_CA_In_Payment:CCCLIENT:C_CA_CCCLIENT:2",
                 "str:value:CA_In_Payment:V_F_CA_In_Payment:CCCLIENT:C_CA_CCCLIENT:3")
    )
  )
  build_stat_xplore_body(database, measures, dimensions, recodes) %>%
    scrape_from_stat_xplore(lookback)
}

get_scotland_PIP_in_payment = function(lookback = 6) {
  database = "str:database:PIP_Monthly"
  measures = "str:count:PIP_Monthly:V_F_PIP_MONTHLY"
  dimensions = c("str:field:PIP_Monthly:F_PIP_DATE:DATE2",
                 "str:field:PIP_Monthly:V_F_PIP_MONTHLY:SEX",
                 "str:field:PIP_Monthly:V_F_PIP_MONTHLY:DL_AWARD_TYPE",
                 "str:field:PIP_Monthly:V_F_PIP_MONTHLY:MOB_AWARD_TYPE",
                 "str:field:PIP_Monthly:V_F_PIP_MONTHLY:SINGLE_AGE",
                 "str:field:PIP_Monthly:V_F_PIP_MONTHLY:LA_code",
                 "str:field:PIP_Monthly:V_F_PIP_MONTHLY:REASSESSMENT_IND")
  recodes = list(
    `str:field:PIP_Monthly:V_F_PIP_MONTHLY:LA_code` = list(
      map = list("str:value:PIP_Monthly:V_F_PIP_MONTHLY:LA_code:V_C_GEOG00_COUNTRY_to_GB:S92000003")),
    `str:field:PIP_Monthly:V_F_PIP_MONTHLY:SINGLE_AGE` = auto_recodes(
      "str:valueset:PIP_Monthly:V_F_PIP_MONTHLY:SINGLE_AGE:C_PIP_SINGLE_AGE"))
  build_stat_xplore_body(
    database,
    measures,
    c(dimensions,"str:field:PIP_Monthly:V_F_PIP_MONTHLY:DURATIONS"), 
    c(recodes,
      list(`str:field:PIP_Monthly:V_F_PIP_MONTHLY:DURATIONS` = list(
        map = list("str:value:PIP_Monthly:V_F_PIP_MONTHLY:DURATIONS:C_PIP_DURATION:1",
                   "str:value:PIP_Monthly:V_F_PIP_MONTHLY:DURATIONS:C_PIP_DURATION:2",
                   "str:value:PIP_Monthly:V_F_PIP_MONTHLY:DURATIONS:C_PIP_DURATION:3"))))) %>% 
    scrape_from_stat_xplore(lookback) %>% 
    bind_rows(build_stat_xplore_body(database, measures, dimensions, recodes) %>%
                scrape_from_stat_xplore(lookback))
}

get_scotland_DLA_in_payment = function(lookback = 10) {
  database = "str:database:DLA_In_Payment"
  measures = "str:count:DLA_In_Payment:V_F_DLA_In_Payment"
  dimensions = c("str:field:DLA_In_Payment:F_DLA_QTR:DATE_NAME",
                 "str:field:DLA_In_Payment:V_F_DLA_In_Payment:CCSEX",
                 "str:field:DLA_In_Payment:V_F_DLA_In_Payment:CAREPAY",
                 "str:field:DLA_In_Payment:V_F_DLA_In_Payment:CCMOBPAY",
                 "str:field:DLA_In_Payment:V_F_DLA_In_Payment:CNAGE",
                 "str:field:DLA_In_Payment:V_F_DLA_In_Payment:PARLC_code")
  recodes = list(
    `str:field:DLA_In_Payment:V_F_DLA_In_Payment:PARLC_code` = list(
      map = list("str:value:DLA_In_Payment:V_F_DLA_In_Payment:PARLC_code:V_C_GEOG00_COUNTRY_to_GB:S92000003")),
    `str:field:DLA_In_Payment:V_F_DLA_In_Payment:CNAGE` = auto_recodes(
      "str:valueset:DLA_In_Payment:V_F_DLA_In_Payment:CNAGE:C_DLA_SINGLE_AGE"))
  build_stat_xplore_body(
    database,
    measures,
    c(dimensions,"str:field:DLA_In_Payment:V_F_DLA_In_Payment:CTDURTN"), 
    c(recodes,
      list(`str:field:DLA_In_Payment:V_F_DLA_In_Payment:CTDURTN` = list(
        map = list("str:value:DLA_In_Payment:V_F_DLA_In_Payment:CTDURTN:C_DLA_DURATION:1",
                   "str:value:DLA_In_Payment:V_F_DLA_In_Payment:CTDURTN:C_DLA_DURATION:2",
                   "str:value:DLA_In_Payment:V_F_DLA_In_Payment:CTDURTN:C_DLA_DURATION:3"))))) %>% 
    scrape_from_stat_xplore(lookback) %>% 
    bind_rows(build_stat_xplore_body(database, measures, dimensions, recodes) %>%
                scrape_from_stat_xplore(lookback))
}

get_scotland_DLA_amounts = function(lookback = 10) {
  database = "str:database:DLA_In_Payment"
  measures = "str:statfn:DLA_In_Payment:V_F_DLA_In_Payment:CAWKLYAMT:MEAN"
  dimensions = c("str:field:DLA_In_Payment:F_DLA_QTR:DATE_NAME",
                 "str:field:DLA_In_Payment:V_F_DLA_In_Payment:CCSEX",
                 "str:field:DLA_In_Payment:V_F_DLA_In_Payment:CAREPAY",
                 "str:field:DLA_In_Payment:V_F_DLA_In_Payment:CCMOBPAY",
                 "str:field:DLA_In_Payment:V_F_DLA_In_Payment:CNAGE",
                 "str:field:DLA_In_Payment:V_F_DLA_In_Payment:PARLC_code")
  recodes = list(
    `str:field:DLA_In_Payment:V_F_DLA_In_Payment:PARLC_code` = list(
      map = list("str:value:DLA_In_Payment:V_F_DLA_In_Payment:PARLC_code:V_C_GEOG00_COUNTRY_to_GB:S92000003")),
    `str:field:DLA_In_Payment:V_F_DLA_In_Payment:CNAGE` = auto_recodes(
      "str:valueset:DLA_In_Payment:V_F_DLA_In_Payment:CNAGE:C_DLA_SINGLE_AGE"))
  build_stat_xplore_body(
    database,
    measures,
    c(dimensions,"str:field:DLA_In_Payment:V_F_DLA_In_Payment:CTDURTN"), 
    c(recodes,
      list(`str:field:DLA_In_Payment:V_F_DLA_In_Payment:CTDURTN` = list(
        map = list("str:value:DLA_In_Payment:V_F_DLA_In_Payment:CTDURTN:C_DLA_DURATION:1",
                   "str:value:DLA_In_Payment:V_F_DLA_In_Payment:CTDURTN:C_DLA_DURATION:2",
                   "str:value:DLA_In_Payment:V_F_DLA_In_Payment:CTDURTN:C_DLA_DURATION:3"))))) %>% 
    scrape_from_stat_xplore(lookback) %>% 
    bind_rows(build_stat_xplore_body(database, measures, dimensions, recodes) %>%
                scrape_from_stat_xplore(lookback))
}