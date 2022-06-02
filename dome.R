# general data check function, every function will be passed through
# this function, which convert date as Date function to be able to be
# filtered, KCB data has data format as %y-%m

# Check functions checking installed packages and install require packages
# KCB.CHECK takes only KCB data, which are civil data, self.employed and
# migrants-in and out data.

sql.import = function(database, list.object){

  require(RPostgreSQL)
  if (!is.character(database) & !is.character(list.object)){
    print("One of passed argument is not a string")

    database = as.character(database)
    list.object = as.character(list.object)
  }
  if (!require.packages %in% rownames(installed.packages()))
  {
    stop(require.packages," ", "is needed")
  }

  dbconnect = RPostgreSQL::dbConnect(drv =PostgreSQL(),
                                     user="postgres",
                                     password="postgres1!",
                                     host="211.48.245.7",
                                     port=5435,
                                     dbname= database)
  command = "SELECT * FROM"
  query = RPostgreSQL::dbGetQuery(dbconnect, paste(command,
                                                   list.object))
  return(query)
}

kcb.check = function(data){

  packages = c("tidyverse", "lme4",
               "MASS", "lubridate", "BMA", "zoo",
               "caret", "CreditRisk", "bnclassify")

  package.check = lapply(packages,
                         function(x)
                         {
                           if (!require(x, character.only = TRUE)){
                             install.packages(x, dependencies = TRUE)
                             library(x, character.only = TRUE)
                           }
                         })

  if (!is.data.frame(data)){
    data = as_tibble(data)
  }

  if("X" %in% colnames(data)){
    data = data %>%
      dplyr::select(-X)
  }

  colnames = c("bs_yr_mon", "gb", "blk_id",
               "ntnl_cd", "gender_cd", "age_cd",
               "cust_cnt", "econ_cnt", "highend_cnt",
               "rt_mdl_gb_410", "rt_mdl_gb_420", "rt_mdl_gb_430",
               "rt_mdl_gb_440", "rt_mdl_gb_510", "rt_mdl_gb_520",
               "rt_mdl_gb_910", "apt_res_cnt", "napt_res_cnt",
               "own_hous_rt", "nown_hous_rt", "hom_com_dist",
               "avg_icm", "avg_icm_o70", "med_icm",
               "rt_icm_1", "rt_icm_2", "rt_icm_3",
               "rt_icm_4", "rt_icm_5", "rt_icm_6",
               "rt_icm_7", "tot_use_amt", "tot_sp_use_amt",
               "tot_ful_paymt_use_amt", "tot_instl_use_amt", "tot_ca_use_amt",
               "tot_sincd_use_amt", "tot_chkcd_use_amt", "tot_use_amt_m12",
               "avg_use_amt_m12", "tot_abrd_amt", "cd_cnt",
               "sincd_cnt", "chkcd_cnt", "avg_cd_cnt",
               "avg_sincd_cnt", "avg_chkcd_cnt", "bal_cnt",
               "bnk_bal_cnt", "nbnk_bal_cnt", "crdt_bal_cnt",
               "hous_bal_cnt", "mrtg1_bal_cnt", "mrtg2_bal_cnt",
               "avg_bal_cnt", "avg_bnk_bal_cnt", "avg_nbnk_bal_cnt",
               "avg_crdt_bal_cnt", "avg_hous_bal_cnt", "avg_mrtg1_bal_cnt",
               "avg_mrtg2_bal_cnt", "tot_bal_amt", "tot_bnk_bal_amt",
               "tot_nbnk_bal_amt", "tot_crdt_bal_amt", "tot_hous_bal_amt",
               "tot_mrtg1_bal_amt", "tot_mrtg2_bal_amt", "avg_bal_amt",
               "avg_bnk_bal_amt", "avg_nbnk_bal_amt", "avg_crdt_bal_amt",
               "avg_hous_bal_amt", "avg_mrtg1_bal_amt", "avg_mrtg2_bal_amt",
               "tot_amt_pi_m12", "tot_amt_pi_m0", "re_debt_amt",
               "avg_dlq_cnt", "avg_dlq_days", "avg_dlq_amt",
               "med_dlq_amt", "dlq0_cnt", "dlq30_cnt",
               "dlq90_cnt", "avg_scr", "rt_scr1",
               "rt_scr2", "rt_scr3", "own_hous_cnt",
               "plu_hous_cnt", "avg_asst_amt", "net_asst_amt",
               "deposit_amt", "car_sz01_cnt", "car_sz02_cnt",
               "car_sz03_cnt", "car_sz04_cnt", "car_sz05_cnt",
               "car_dmst_cnt", "car_frgn_cnt", "car_new_cnt",
               "car_used_cnt", "avg_car_amt",  "rt_lse_own",
               "ltv_bal", "i_bal_cnt", "i_avg_bal_cnt",
               "i_avg_bal_amt", "i_cd_cnt", "i_use_amt")

  cols = base::tolower(colnames(data))

  if (!any((cols %in% colnames))){
    stop("EmotionalDome thinks you have made a mistake. \n
          He became extremely emotional..")
  }

  colnames(data) = cols
  data[is.na(data)] = 0

  if ("gb" %in% colnames(data)){

    cat("LOADING CIVIL DATA")
    data$bs_yr_mon = lubridate::ym(data$bs_yr_mon)
    return(data)
  }

  if ("st_mon" %in% colnames(data)){

    cat("LOADING SELF-EMPLOYED DATA")

    try({
      data$st_mon = stringr::str_remove(data$st_mon, 'ì›”')
    })
    try({
      data$st_mon = stringr::str_remove(data$st_mon, '월')
    })

    data$st_yr_mon = paste(data$st_yr, data$st_mon)
    data$st_yr_mon = lubridate::ym(data$st_yr_mon)

    return(data)
  }

  else{
    stop("Truth Dome thinks that the data you have is not true")
  }

  return(data)
}

old.own.hous = function(data, target = c("ADM_DONG", "GR100M", "ZIP_CD"),
                        month = FALSE){
  data = kcb.check(data)

  sub.data = data %>%
    dplyr::filter(gb == target)

  dates = unique(sub.data$bs_yr_mon)
  months = unique(lubridate::month(dates))

  if (month){

    list = list()

    if (length(unique(lubridate::year(dates))) > 1L){
      stop("!?")
    }

    for (i in 1L:12){
      list[[i]] = sub.data %>%
        dplyr::group_by(blk_id) %>%
        dplyr::filter(lubridate::month(bs_yr_mon) == months[i]) %>%
        dplyr::mutate(hous = sum(own_hous_cnt)) %>%
        dplyr::mutate(cust = sum(cust_cnt)) %>%
        dplyr::summarise(unique(hous / cust))
      output = purrr::reduce(list, dplyr::full_join, by = "blk_id")
    }

    columns = paste("t", 1L:12, sep = "")
    columns = append(columns, "blk_id", 0)
    colnames(output) = columns

    return(output)
  }

  else{

    output = sub.data %>%
      dplyr::group_by(blk_id) %>%
      dplyr::mutate(hous = sum(own_hous_cnt)) %>%
      dplyr::mutate(cust = sum(cust_cnt)) %>%
      dplyr::summarise(unique(hous / cust))

    return(output)
  }
}

old.asst.bal = function(data, target = c("ADM_DONG", "GR100M", "ZIP_CD"),
                        month = FALSE){
  data = kcb.check(data)

  sub.data = data %>%
    dplyr::filter(gb == target)

  dates = unique(sub.data$bs_yr_mon)
  months = unique(lubridate::month(dates))

  if (month){

    list = list()

    if (length(unique(lubridate::year(dates))) > 1L){
      stop("!?")
    }

    for (i in 1L:12){
      list[[i]] = sub.data %>%
        dplyr::group_by(blk_id) %>%
        dplyr::filter(lubridate::month(bs_yr_mon) == months[i]) %>%
        dplyr::mutate(bal = sum(avg_bal_amt)) %>%
        dplyr::mutate(asst = sum(avg_asst_amt)) %>%
        dplyr::summarise(unique(bal / asst))
      output = purrr::reduce(list, dplyr::full_join, by = "blk_id")
    }

    columns = paste("t", 1L:12, sep = "")
    columns = append(columns, "blk_id", 0)
    colnames(output) = columns

    return(output)
  }

  else{

    output = sub.data %>%
      dplyr::group_by(blk_id) %>%
      dplyr::mutate(bal = sum(avg_bal_amt)) %>%
      dplyr::mutate(asst = sum(avg_asst_amt)) %>%
      dplyr::summarise(unique(bal / asst))

    return(output)
  }
}

old.bnk.bal = function(data, target = c("ADM_DONG", "GR1L00M", "ZIP_CD"),
                       month = FALSE){
  data = kcb.check(data)

  sub.data = data %>%
    dplyr::filter(gb == target)

  dates = unique(sub.data$bs_yr_mon)
  months = unique(lubridate::month(dates))

  if (month){

    list = list()

    if (length(unique(lubridate::year(dates))) > 1L){
      stop("!?")
    }

    for (i in 1L:12){
      list[[i]] = sub.data %>%
        dplyr::group_by(blk_id) %>%
        dplyr::filter(lubridate::month(bs_yr_mon) == months[i]) %>%
        dplyr::mutate(bnk = sum(tot_bnk_bal_amt)) %>%
        dplyr::mutate(bal = sum(tot_bal_amt)) %>%
        dplyr::summarise(unique(bnk / bal))
      output = purrr::reduce(list, dplyr::full_join, by = "blk_id")
    }

    columns = paste("t", 1L:12, sep = "")
    columns = append(columns, "blk_id", 0)
    colnames(output) = columns

    return(output)
  }

  else{

    output = sub.data %>%
      dplyr::group_by(blk_id) %>%
      dplyr::mutate(bnk = sum(tot_bnk_bal_amt)) %>%
      dplyr::mutate(bal = sum(tot_bal_amt)) %>%
      dplyr::summarise(unique(bnk / bal))

    return(output)
  }
}

old.repr.ln.p1 = function(data){

  data = kcb.check(data)

  sub.data = data %>%
    dplyr::group_by(sic_clsfy) %>%
    dplyr::summarise(sum(avg_ca_use_amt * biz_cnt) / sum(biz_cnt))

  return(sub.data)
}

old.repr.rvn.bal = function(data){

  data = kcb.check(data)

  sub.data = data %>%
    dplyr::group_by(sic_clsfy) %>%
    dplyr::summarise((sum(avg_ln_bal * biz_cnt) / sum(biz_cnt)) /
                       (sum(avg_rvn * biz_cnt) / sum(biz_cnt)))

  return(sub.data)
}

old.repr.ca.use = function(data){

  data = kcb.check(data)

  sub.data = data %>%
    dplyr::group_by(sic_clsfy) %>%
    dplyr::summarise(sum(avg_ca_use_amt * biz_cnt) / sum(biz_cnt))

  return(sub.data)
}

# Scan index stating point
# total income index and total debt index are calculated as percentage
# In KCB data, we have only 3 types of income sources.
# AVG, MED, and 0.3 quantile, Consequently, we take median income here.

scn.icm = function(data, target = c("ADM_DONG", "ZIP_CD"), rank = FALSE){

  data = kcb.check(data)

  sub.data = data %>%
    dplyr::filter(gb == "ADM_DONG")

  months = unique(lubridate::month(sub.data$bs_yr_mon))
  list = list()

  for (i in 1L:12){
    list[[i]] = sub.data %>%
      dplyr::group_by(blk_id) %>%
      dplyr::filter(lubridate::month(bs_yr_mon) == months[i]) %>%
      dplyr::summarise(median(med_icm))
  }

  output = purrr::reduce(list, dplyr::full_join, by = "blk_id")
  cols = paste('t', 1L:12, sep = "")
  cols = append(cols, "blk_id", 0)
  colnames(output) = cols

  if (rank){

    rank_output = apply(output[, 2:13], 2, function(x) rank(-x))
    return(rank_output)
  }

  return (output)
}

scn.bal = function(data, target = c("ADM_DONG", "ZIP_CD"), rank = FALSE){

  data = kcb.check(data)

  sub.data = data %>%
    dplyr::filter(gb == target)

  months = unique(lubridate::month(sub.data$bs_yr_mon))
  list = list()

  for (i in 1L:12){
    list[[i]] = sub.data %>%
      dplyr::group_by(blk_id) %>%
      dplyr::filter(lubridate::month(bs_yr_mon) == months[i]) %>%
      dplyr::summarise(median(avg_bal_amt))
  }

  output = purrr::reduce(list, dplyr::full_join, by = "blk_id")
  cols = paste('t', 1L:12, sep = "")
  cols = append(cols, "blk_id", 0)
  colnames(output) = cols

  if (rank){

    rank_output = apply(output[, 2:13], 2, function(x) rank(-x))
    return(rank_output)
  }

  return (output)
}

scn.consumpt = function(data, target = c("ADM_DONG", "GR100M", "ZIP_CD")){

  data = kcb.check(data)

  sub.data = data %>%
    dplyr::filter(gb == target)

  months = unique(lubridate::month(sub.data$bs_yr_mon))
  list = list()

  if (target == "GR100M"){

    sub.data = data %>%
      dplyr::filter(gb == "GR100M")

    months = unique(lubridate::month(sub.data$bs_yr_mon))
    list = list()

    for (i in 1L:12){
      list[[i]] = sub.data %>%
        dplyr::group_by(blk_id) %>%
        dplyr::filter(lubridate::month(bs_yr_mon) == months[i]) %>%
        dplyr::summarise(median(avg_use_amt_m12))
    }

    output = purrr::reduce(list, dplyr::full_join, by = "blk_id")
    cols = paste('t', 1L:12, sep = "")
    cols = append(cols, "blk_id", 0)
    colnames(output) = cols
    return(output)
  }

  if (target == "ADM_DONG"){

    sub.data = data %>%
      dplyr::filter(gb == "ADM_DONG")

    months = unique(lubridate::month(sub.data$bs_yr_mon))
    list = list()

    for (i in 1L:12){
      list[[i]] = sub.data %>%
        dplyr::group_by(blk_id) %>%
        dplyr::filter(lubridate::month(bs_yr_mon) == months[i]) %>%
        dplyr::summarise(median(avg_use_amt_m12))
    }

    output = purrr::reduce(list, dplyr::full_join, by = "blk_id")
    cols = paste('t', 1L:12, sep = "")
    cols = append(cols, "blk_id", 0)
    colnames(output) = cols
    return(output)
  }

  if (target == "ZIP_CD"){

    sub.data = data %>%
      dplyr::filter(gb == "ZIP_CD")

    months = unique(lubridate::month(sub.data$bs_yr_mon))
    list = list()

    for (i in 1L:12){
      list[[i]] = sub.data %>%
        dplyr::group_by(blk_id) %>%
        dplyr::filter(lubridate::month(bs_yr_mon) == months[i]) %>%
        dplyr::summarise(median(avg_use_amt_m12))
    }

    output = purrr::reduce(list, dplyr::full_join, by = "blk_id")
    cols = paste('t', 1L:12, sep = "")
    cols = append(cols, "blk_id", 0)
    colnames(output) = cols
    return(output)
  }
}

scn.savings = function(data, target = c("ADM_DONG", "ZIP_CD")){

  data = kcb.check(data)

  sub.data = data %>%
    dplyr::filter(gb == target)

  months = unique(lubridate::month(sub.data$bs_yr_mon))
  list = list()

  for (i in 1L:12){
    list[[i]] = sub.data %>%
      dplyr::group_by(blk_id) %>%
      dplyr::filter(lubridate::month(bs_yr_mon) == months[i]) %>%
      dplyr::summarise(median(deposit_amt))
  }

  output = purrr::reduce(list, dplyr::full_join, by = "blk_id")
  cols = paste('t', 1L:12, sep = "")
  cols = append(cols, "blk_id", 0)
  colnames(output) = cols

  return (output)
}

scn.interval.icm = function(data, target = c("ADM_DONG", "ZIP_CD")){

  data = kcb.check(data)

  sub.data = data %>%
    dplyr::filter(gb == target)

  quant.seq = seq(0, 1L, 0.1)
  list = list()

  for (i in 1L:(length(quant.seq) - 1L)){
    list[[i]] = sub.data %>%
      dplyr::group_by(blk_id) %>%
      dplyr::filter(med_icm  <= quantile(med_icm, quant.seq[i + 1L]) &
                      med_icm > quant.seq[i]) %>%
      dplyr::summarise(sum(cust_cnt))
  }

  output = purrr::reduce(list, dplyr::full_join, by = "blk_id")
  cols = paste('t', 1L:12, sep = "")
  cols = append(cols, "blk_id", 0)
  colnames(output) = cols

  return(output)
}

scn.vol.icm = function(data, target = c("ADM_DONG", "ZIP_CD")){

  data = kcb.check(data)

  sub.data = data %>%
    dplyr::filter(gb == target)

  blk_id = unique(sub.data$blk_id)
  months = unique(lubridate::month(sub.data$bs_yr_mon))
  list = list()

  for (i in 1L:12){
    list[[i]] = sub.data %>%
      dplyr::group_by(blk_id) %>%
      dplyr::filter(lubridate::month(bs_yr_mon) == months[i]) %>%
      dplyr::summarise(mean(med_icm))
  }

  output = purrr::reduce(list, dplyr::full_join, by = "blk_id")
  ret = apply(output[, 2:13], 1, function(x) (log(x) - log(lag(x))) * 100)
  vol = apply(ret, 2, function(x) sd(x, na.rm = TRUE))
  results = as_tibble(cbind(blk_id, as.numeric(vol)))
  colnames(results) = c("blk_id", "Volatility")

  return(results)
}

scn.vol.bal = function(data, target = c("ADM_DONG", "ZIP_CD")){

  data = kcb.check(data)

  sub.data = data %>%
    dplyr::filter(gb == target)

  blk_id = unique(sub.data$blk_id)
  months = unique(lubridate::month(sub.data$bs_yr_mon))
  list = list()

  for (i in 1L:12){
    list[[i]] = sub.data %>%
      dplyr::group_by(blk_id) %>%
      dplyr::filter(lubridate::month(bs_yr_mon) == months[i]) %>%
      dplyr::summarise(mean(avg_bal_amt))
  }

  output = purrr::reduce(list, dplyr::full_join, by = "blk_id")
  ret = apply(output[, 2:13], 1, function(x) (log(x) - log(lag(x))) * 100)
  vol = apply(ret, 2, function(x) sd(x, na.rm = TRUE))
  results = as_tibble(cbind(blk_id, as.numeric(vol)))
  colnames(results) = c("blk_id", "Volatility")

  return(results)
}

scn.vol.consumpt = function(data, target = c("ADM_DONG", "GR100M","ZIP_CD")){

  data = kcb.check(data)

  if (target == "GR100M"){

    sub.data = data %>%
      dplyr::filter(gb == "GR100M")

    blk_id = unique(sub.data$blk_id)
    months = unique(lubridate::month(sub.data$bs_yr_mon))
    list = list()

    for (i in 1L:12){
      list[[i]] = sub.data %>%
        dplyr::group_by(blk_id) %>%
        dplyr::filter(lubridate::month(bs_yr_mon) == months[i]) %>%
        dplyr::summarise(mean(avg_bal_amt))
    }

    output = purrr::reduce(list, dplyr::full_join, by = "blk_id")
    ret = apply(output[, 2:13], 1, function(x) (log(x) - log(lag(x))) * 100)
    vol = apply(ret, 2, function(x) sd(x, na.rm = TRUE))
    results = as_tibble(cbind(blk_id, as.numeric(vol)))
    colnames(results) = c("blk_id", "Volatility")

    return(results)
  }

  if (target == "ADM_DONG"){

    sub.data = data %>%
      dplyr::filter(gb == "ADM_DONG")

    blk_id = unique(sub.data$blk_id)
    months = unique(lubridate::month(sub.data$bs_yr_mon))
    list = list()

    for (i in 1L:12){
      list[[i]] = sub.data %>%
        dplyr::group_by(blk_id) %>%
        dplyr::filter(lubridate::month(bs_yr_mon) == months[i]) %>%
        dplyr::summarise(mean(avg_bal_amt))
    }

    output = purrr::reduce(list, dplyr::full_join, by = "blk_id")
    ret = apply(output[, 2:13], 1, function(x) (log(x) - log(lag(x))) * 100)
    vol = apply(ret, 2, function(x) sd(x, na.rm = TRUE))
    results = as_tibble(cbind(blk_id, as.numeric(vol)))
    colnames(results) = c("blk_id", "Volatility")

    return(results)
  }

  if (target == "ZIP_CD"){

    sub.data = data %>%
      dplyr::filter(gb == "ZIP_CD")

    blk_id = unique(sub.data$blk_id)
    months = unique(lubridate::month(sub.data$bs_yr_mon))
    list = list()

    for (i in 1L:12){
      list[[i]] = sub.data %>%
        dplyr::group_by(blk_id) %>%
        dplyr::filter(lubridate::month(bs_yr_mon) == months[i]) %>%
        dplyr::summarise(mean(avg_bal_amt))
    }

    output = purrr::reduce(list, dplyr::full_join, by = "blk_id")
    ret = apply(output[, 2:13], 1, function(x) (log(x) - log(lag(x))) * 100)
    vol = apply(ret, 2, function(x) sd(x, na.rm = TRUE))
    results = as_tibble(cbind(blk_id, as.numeric(vol)))
    colnames(results) = c("blk_id", "Volatility")

    return(results)
  }
}

scn.vol.savings = function(data, target = c("ADM_DONG", "ZIP_CD")){

  data = kcb.check(data)

  sub.data = data %>%
    dplyr::filter(gb == target)

  blk_id = unique(sub.data$blk_id)
  months = unique(lubridate::month(sub.data$bs_yr_mon))
  list = list()

  for (i in 1L:12){
    list[[i]] = sub.data %>%
      dplyr::group_by(blk_id) %>%
      dplyr::filter(lubridate::month(bs_yr_mon) == months[i]) %>%
      dplyr::summarise(mean(deposit_amt))
  }

  output = purrr::reduce(list, dplyr::full_join, by = "blk_id")
  ret = apply(output[, 2:13], 1, function(x) (log(x) - log(lag(x))) * 100)
  vol = apply(ret, 2, function(x) sd(x, na.rm = TRUE))
  results = as_tibble(cbind(blk_id, as.numeric(vol)))
  colnames(results) = c("blk_id", "Volatility")

  return(results)
}

self.scn.icm = function(data){

  data = kcb.check(repr)

  months = unique(lubridate::month(data$st_yr_mon))
  sic = unique(data$sic_clsfy)

  list = list()

  for (i in 1:length(months)){
    list[[i]] = data %>%
      dplyr::group_by(sic_clsfy) %>%
      dplyr::filter(lubridate::month(st_yr_mon) == months[i]) %>%
      dplyr::summarise(median(med_icm))
  }

  output = purrr::reduce(list, full_join, by = "sic_clsfy")

  cols = paste("t", 1L:12, sep = "")
  cols = append(cols, "sic_clsfy", 0)
  colnames(output) = cols

  return(output)
}

self.scn.rvn = function(data){

  data = kcb.check(repr)

  months = unique(lubridate::month(data$st_yr_mon))
  sic = unique(data$sic_clsfy)

  list = list()

  for (i in 1:length(months)){
    list[[i]] = data %>%
      dplyr::group_by(sic_clsfy) %>%
      dplyr::filter(lubridate::month(st_yr_mon) == months[i]) %>%
      dplyr::summarise(median(med_rvn))
    }

    output = purrr::reduce(list, full_join, by = "sic_clsfy")

    cols = paste("t", 1L:12, sep = "")
    cols = append(cols, "sic_clsfy", 0)
    colnames(output) = cols

    return(output)
}

self.scn.bal = function(data){

  data = kcb.check(repr)

  months = unique(lubridate::month(data$st_yr_mon))
  sic = unique(data$sic_clsfy)

  list = list()

  for (i in 1:length(months)){
    list[[i]] = data %>%
      dplyr::group_by(sic_clsfy) %>%
      dplyr::filter(lubridate::month(st_yr_mon) == months[i]) %>%
      dplyr::summarise(median(avg_ln_bal))
  }

  output = purrr::reduce(list, full_join, by = "sic_clsfy")

  cols = paste("t", 1L:12, sep = "")
  cols = append(cols, "sic_clsfy", 0)
  colnames(output) = cols

  return(output)
}

# Analyze Index start.
# DTI (Debt-to-income)

dti.leverage = function(data, target = c("ADM_DONG", "ZIP_CD")){

  data = kcb.check(data)

  sub.data = data %>%
    dplyr::filter(gb == target)

  # average total debt and average total income of households are respectively
  # total debt and total income divided by population
  # tot_amt_pi_m12 and m0 are respectively total debt repaid a year by households
  # in order to increase accuracy of discount rate, we take total 12 month debt repaid

  dti = sub.data %>%
    dplyr::group_by(blk_id) %>%
    dplyr::mutate(dti = (avg_bal_amt - (tot_amt_pi_m12 / bal_cnt))/ avg_icm)

  # Since infinite values are caused by people who do not have debt
  # replacing it by 0 is not an error.

  dti$dti[is.infinite(dti$dti)] = 0

  output = na.omit(dti) %>%
    dplyr::summarise(median(dti, na.rm = TRUE))

  return(output)
}

dti.crit.leverage = function(data, target = c("ADM_DONG", "ZIP_CD")){

  data = kcb.check(data)

  spearman.cor = function(x){
    stats::cor(x, method = "spearman")
  }

  sub.data = na.omit(data) %>%
    dplyr::filter(gb == "ADM_DONG")

  # debt decomposition takes every variables related to households debt.
  # total debts denoted as tot prefix average debt are the values correspond
  # to their total values divided by population size.

  blk_id = unique(sub.data$blk_id)

  bal.decomp = sub.data %>%
    dplyr::select(avg_bnk_bal_amt, avg_nbnk_bal_amt,
                  avg_crdt_bal_amt, avg_hous_bal_amt,
                  avg_mrtg1_bal_amt, avg_mrtg2_bal_amt,
                  dlq90_cnt, blk_id)

  debt.mat = sub.data %>%
    dplyr::select(blk_id, avg_bnk_bal_amt, avg_nbnk_bal_amt,
                  avg_crdt_bal_amt, avg_hous_bal_amt,
                  avg_mrtg1_bal_amt, avg_mrtg2_bal_amt)

  # Splitting unique administrative division.

  list = list()
  icm.list = list()
  cor.list = list()
  debt.list = list()

  for (i in 1L:length(blk_id)){
    list[[i]] = bal.decomp[bal.decomp$blk_id == blk_id[i], ]
    icm.list[[i]] = sub.data[sub.data$blk_id == blk_id[i], ]$avg_icm
    debt.list[[i]] = debt.mat[debt.mat$blk_id == blk_id[i], ]
  }

  # After filtering removing BLK_ID

  debt.list = lapply(debt.list, function(x) x[(names(x) %in% c(
    "avg_bnk_bal_amt", "avg_nbnk_bal_amt",
    "avg_crdt_bal_amt", "avg_hous_bal_amt",
    "avg_mrtg1_bal_amt", "avg_mrtg2_bal_amt"
  ))])

  new.list = lapply(list, function(x) x[(names(x) %in% c(
    "avg_bnk_bal_amt", "avg_nbnk_bal_amt",
    "avg_crdt_bal_amt", "avg_hous_bal_amt",
    "avg_mrtg1_bal_amt", "avg_mrtg2_bal_amt",
    "dlq90_cnt"
  ))])

  cor.list = lapply(new.list, function(x) spearman.cor(x))

  for (i in 1L:length(blk_id)){
    cor.list[[i]] = cor.list[[i]][(dim(bal.decomp)[2] - 1L), 1L:(dim(bal.decomp)[2] - 2)]
  }

  # weight matrix construction

  w.mat = lapply(cor.list, function(x)  x / sum(x))
  w.mat = lapply(w.mat, as.matrix)
  d.mat = lapply(debt.list, as.matrix)
  # dimension check

  if(dim(w.mat[[1L]])[1L] != dim(d.mat[[1L]])[2]){
    stop("Dimension Error")
  }

  # Debt matrix operation.

  d.mat.w = list()
  t.mat.w = lapply(w.mat, t)
  t.mat.d = lapply(d.mat, t)

  for (i in 1L:length(blk_id)){
    d.mat.w[[i]] = t.mat.w[[i]] %*% t.mat.d[[i]]
  }

  b.output = as.numeric(unlist(d.mat.w))
  output = as.data.frame(cbind(sub.data$blk_id, b.output, sub.data$avg_bal_amt))

  colnames(output) = c("blk_id", "weighted_bal", "avg_bal_amt")

  output[output == 0] = 1
  output$weighted_bal = as.numeric(output$weighted_bal)
  output$avg_bal_amt = as.numeric(output$avg_bal_amt)

  results = output %>%
    dplyr::mutate(results = weighted_bal / avg_bal_amt) %>%
    dplyr::group_by(blk_id) %>%
    dplyr::summarise(median(results))

  return(results)
}

dti.threshold = function(data, target = c("ADM_DONG", "ZIP_CD")){

  data = kcb.check(data)

  sub.data = data %>%
    dplyr::filter(gb == target)

  cd_code = unique(sub.data$blk_id)

  dti = sub.data %>%
    dplyr::mutate(dti = (avg_bal_amt - (tot_amt_pi_m12 / bal_cnt)) / avg_icm)

  dti$dti[is.infinite(dti$dti)] = 0

  tryCatch({
    lmer.fit = lme4::lmer(dti ~ 1L + dlq90_cnt
                          + (1L|blk_id) + (1L|bs_yr_mon), data = na.omit(dti))

    if(lme4::isSingular(lmer.fit)){
      cat("First try on LMM, boundary seems to be singular \n")
      cat("Removing Random-Effects and refitting")
    }

  }, silent = TRUE,
  finally =  {
    lmer.fit = lme4::lmer(dti ~ 1L + dlq90_cnt
                          + (1L|blk_id), data = na.omit(dti))
  })

  lmer = as.data.frame(coef(lmer.fit)[1L])

  thresholds = lmer[1L] + 1L * lmer[2]

  return(thresholds)
}

dti.vulnerable = function(data, target = c("ADM_DONG", "GR100M", "ZIP_CD")){

  data = kcb.check(data)

  sub.data = data %>%
    dplyr::filter(gb == target)

  # Legally speaking, extreme lower-income can be considered
  # q0.09.

  qt009 = quantile(sub.data$avg_icm, 0.09)

  mat = sub.data[sub.data$avg_icm < qt009, ]
  mat$dti = (mat$avg_bal_amt -
               (mat$tot_amt_pi_m12 / mat$bal_cnt)) / mat$avg_icm

  threshold = max(unlist(density(na.contiguous(mat$dti))[2]))

  output = mat %>%
    dplyr::group_by(blk_id) %>%
    dplyr::filter(dti >= threshold) %>%
    dplyr::summarise(sum(dlq90_cnt))

  return(output)
}
'here to start'

dti.bankruptcy = function(data){

  prob.convert = function(binom.coef){
    odds = exp(binom.coef)
    probs = odds / (1L + odds)
    return(probs)
  }

  data = kcb.check(data)

  sub.data = data %>%
    dplyr::filter(gb == "ADM_DONG") %>%
    dplyr::mutate(dti = (avg_bal_amt - (tot_amt_pi_m12 / bal_cnt))/ avg_icm)

  sub.data = na.omit(sub.data)

  blk_id = sub.data$blk_id
  nums = unlist(lapply(sub.data, is.numeric))
  num.cols = unlist(lapply(sub.data, is.numeric))
  subset = sub.data[, num.cols]

  # this horrible passive removing variable is to accelerate
  # BMA calculation.

  subset.fi = subset %>%
    dplyr::select(-gender_cd,
                  -rt_mdl_gb_410, -rt_mdl_gb_420,
                  -rt_mdl_gb_430, -rt_mdl_gb_440,
                  -rt_mdl_gb_510, -rt_mdl_gb_520,
                  -rt_mdl_gb_910, -rt_scr1, -rt_scr2,
                  -rt_scr3, -dlq0_cnt, -dlq30_cnt,
                  -rt_lse_own, -ltv_bal, -i_bal_cnt,
                  -i_avg_bal_amt, -i_cd_cnt, -i_use_amt, -ntnl_cd,
                  -apt_res_cnt, -napt_res_cnt, - hom_com_dist,
                  -avg_icm_o70, -rt_icm_1, -rt_icm_2, -rt_icm_3,
                  -rt_icm_4, -rt_icm_5, -rt_icm_6, -rt_icm_7)

  tmp = cor(subset.fi, method = "spearman")
  tmp[upper.tri(tmp)] = 0
  diag(tmp) = 0

  # to avoid singular system, we remove variables exceeding 0.7

  cor.new = subset.fi[, !apply(tmp, 2, function(x) any(abs(x) > 0.7, na.rm = TRUE))]

  # max linear regression slope selecting to determine threshold

  thresholds = function(){

    list = list()
    lm.list = list()

    seq = seq(0, 1L, 0.1)

    for (i in 1L:length(seq)){
      list[[i]] = subset %>%
        dplyr::filter(dti > quantile(dti, seq[i]) & dti <= quantile(dti, seq[i + 1L]))
    }

    for (i in 1L:length(seq)){
      lm.list[[i]] = cov(list[[i]]$dlq90_cnt, list[[i]]$dti) / var(list[[i]]$dlq90_cnt)
    }

    output = unlist(lm.list)
    index = index(max(output))

    return(index)
  }

  # this is threshold point.
  quantile = as.numeric(quantile(sub.data$dti, (thresholds() / 10), na.rm = TRUE))

  # First target below quantile
  cor.new$target = base::ifelse(cor.new$dti < quantile, 1L, 0)

  cor.subset = cor.new %>%
    dplyr::select(-dti)

  # Applying 2nd filter BMA for 2nd one

  bma.fit = BMA::bic.glm(target ~.,
                         data = na.omit(cor.subset),
                         glm.family = binomial(link = "logit"))

  label = bma.fit$label[1L]
  label = gsub(".x", "", as.character(label))
  label = unlist(strsplit(label, split = ","))

  bma.selected = cor.subset[label]
  bma.selected$target = cor.subset$target



  glm.fit = glm(target ~., family = binomial(link = "logit"),
                data = na.omit(bma.selected))

  bma.cols = colnames(bma.selected)
  bma.cols = bma.cols[1L:(dim(bma.selected)[2] - 1L)]

  s.data = sub.data[bma.cols]
  s.data$blk_id = sub.data$blk_id

  d.mat = s.data %>%
    dplyr::select(-blk_id)

  intercept = rep(1L, dim(d.mat)[1L])
  id.mat = as.matrix(cbind(intercept, d.mat))

  w = t(as.matrix(coef(glm.fit)))
  t.id.mat = t(id.mat)

  coef = as.numeric(w %*% t.id.mat)

  pb.event = 1L / (1L + exp(-coef))

  output = as.data.frame(cbind(s.data$blk_id, pb.event))
  output$pb.event = as.numeric(output$pb.event)

  print(exp(cbind(coef(glm.fit), confint(glm.fit))))

  colnames(output) = c("blk_id", "pb")

  result = na.omit(output) %>%
    dplyr::group_by(blk_id) %>%
    dplyr::summarise(mean(pb))

  return(result)
}

fm = function(data, target = c("ADM_DONG", "ZIP_CD")){

  data = kcb.check(data)

  sub.data = data %>%
    dplyr::filter(gb == "ADM_DONG") %>%
    dplyr::mutate(fm = deposit_amt + avg_asst_amt
                  - ((tot_bal_amt - tot_amt_pi_m12) / bal_cnt))

  if (sum(is.na(sub.data > 0))){
    sub.data[is.na(sub.data)] = 0
  }

  sub.data$fm[(is.infinite(sub.data$fm))] = 0
  blk_id = unique(sub.data$blk_id)
  months = unique(lubridate::month(sub.data$bs_yr_mon))
  list = list()

  for (i in 1L:length(months)){
    list[[i]] = sub.data %>%
      dplyr::group_by(blk_id) %>%
      dplyr::filter(lubridate::month(bs_yr_mon) == months[i]) %>%
      dplyr::summarise(median(fm))
  }

  output = purrr::reduce(list, dplyr::full_join, by = "blk_id")

  columns = paste("t", 1L:12, sep = "")
  columns = append(columns, "blk_id", 0)
  colnames(output) = columns

  return(output)
}

fm.solv.delta = function(data, target = c("ADM_DONG", "ZIP_CD")){

  data = kcb.check(data)

  sub.data = data %>%
    dplyr::filter(gb == target)

  months = 1L:12
  unique_blk_id = unique(sub.data$blk_id)

  list.bal = list()
  list.asst = list()
  list.re.debt = list()

  for (i in 1L:12){
    list.bal[[i]] = sub.data %>%
      dplyr::group_by(blk_id) %>%
      dplyr::filter(lubridate::month(bs_yr_mon) == months[i]) %>%
      dplyr::summarise(mean(avg_bal_amt))
  }

  for (j in 1L:12){
    list.asst[[j]] = sub.data %>%
      dplyr::group_by(blk_id) %>%
      dplyr::filter(lubridate::month(bs_yr_mon) == months[j]) %>%
      dplyr::summarise(mean(avg_asst_amt))
  }

  for (k in 1L:12){
    list.re.debt[[k]] = sub.data %>%
      dplyr::group_by(blk_id) %>%
      dplyr::filter(lubridate::month(bs_yr_mon) == months[k]) %>%
      dplyr::summarise(mean(re_debt_amt))
  }

  bal = reduce(list.bal, dplyr::full_join, by = "blk_id")[, 2:13]
  asst = reduce(list.asst, dplyr::full_join, by = "blk_id")[, 2:13]
  re.bal = reduce(list.re.debt, dplyr::full_join, by = "blk_id")[, 2:13]
  output = cbind(unique_blk_id, ((asst - (bal - re.bal * 12 ))/ bal))

  columns = paste("t", 1L:12, sep = "")
  columns = append(columns, "blk_id", 0)
  colnames(output) = columns

  return(output)
}

fm.threshold = function(data, target = c("ADM_DONG", "ZIP_CD")){

  data = kcb.check(data)

  sub.data = data %>%
    dplyr::filter(gb == target) %>%
    dplyr::mutate(fm = (avg_icm - avg_use_amt_m12) + (avg_asst_amt - avg_bal_amt)) %>%
    dplyr::mutate(fmp = fm / avg_bal_amt) %>%
    na.omit()

  sub.data$fmp[is.infinite(sub.data$fmp)] = 0

  tryCatch(
    {
      lmer.fit = lme4::lmer(fmp ~ 1L + dlq90_cnt
                            + (1L|blk_id) + (1L|bs_yr_mon), data = sub.data)

      if(lme4::isSingular(lmer.fit)){
        cat("First try on LMM, boundary seems to be singular \n")
        cat("Removing Random-Effects and refitting")
      }

      silent = TRUE},

    finally = {
      lmer.fit = lme4::lmer(fmp ~ 1L + dlq90_cnt
                            + (1L|blk_id), data = sub.data)
    }
  )

  lmer = as.data.frame(coef(lmer.fit)[1L])

  thresholds = lmer[1L] + 1L * lmer[2]

  lmer = as.data.frame(coef(lmer.fit)[1L])

  thresholds = lmer[1L] + 1L * lmer[2]

  return(thresholds)
}

fm.dlq.prob = function(data){

  data = kcb.check(data)

  # filtering necessary components to reduce computational stress

  sub.data = data %>%
    dplyr::filter(gb == "ADM_DONG") %>%
    dplyr::mutate(fm = (avg_icm - avg_use_amt_m12) + avg_asst_amt
                  - ((tot_bal_amt - tot_amt_pi_m12) / bal_cnt))

  if (sum(sub.data$bal_cnt) > 0){
    sub.data$bal_cnt[sub.data$bal_cnt == 0] = 1L
  }
  if (sum(is.na(sub.data)) > 0){
    sub.data[is.na(sub.data)] = 0
  }

  if (sum(is.infinite(sub.data$fm)) > 0){
    sub.data$fm[!is.finite(sub.data$fm)] = 0
  }

  threshold = function(){

    array = array(NA)
    seq = seq(0, 1L, 0.05)
    for (i in 1L:(length(seq) -1L)){
      array[i] = subset.data = sub.data %>%
        dplyr::select(fm, dlq90_cnt) %>%
        dplyr::filter(fm >= quantile(fm, seq[i]) & fm < quantile(fm, seq[i + 1L])) %>%
        dplyr::summarise(sum(dlq90_cnt))
    }

    dlq.interval = which(unlist(array) == max(unlist(array)))
    point = seq[dlq.interval]
    return(point)
  }

  sub.data$target = base::ifelse(sub.data$fm >=
                                   quantile(sub.data$fm, threshold()), 1L, 0)

  sample.data = sub.data$fm %>%
    caret::createDataPartition(p = 0.8, list = FALSE)

  train = sub.data[sample.data, ]
  test = sub.data[-sample.data, ]


  glm.fit = glm(target ~ fm, family = binomial(link = "logit"),
                data = train, maxit = 5000)
  glm.fit2 = glm(target ~ fm, family = binomial(link = "logit"),
                 data = sub.data, maxit = 5000)

  probs = glm.fit %>%
    predict(test, type = "response")

  pred = as.data.frame(probs) %>%
    dplyr::mutate(target = base::ifelse(probs >= .5, 1L, 0))

  result = mean(pred$target == test$target)

  if (result > 0.8){

    blk_id = sub.data$blk_id
    intercept = rep(1L, dim(sub.data)[1L])
    mat = as.matrix(cbind(intercept, sub.data$fm))

    w = as.matrix(coef(glm.fit2))
    coef = as.numeric(t(w) %*% t(mat))
    pb.event = 1L / (1L + exp(-coef))

    output = as.data.frame(cbind(blk_id, pb.event))
    output$pb.event = as.numeric(output$pb.event)

    output.df = output %>%
      dplyr::group_by(blk_id) %>%
      dplyr::summarise(mean(pb.event))

    return(output.df)
  }

  else{
    print("Probability Estimation failed")
  }
}

cci.inflation = function(data, target = c("ADM_DONG", "ZIP_CD")){

  data = kcb.check(data)

  sub.data = data %>%
    dplyr::filter(gb == target)

  blk_id = unique(sub.data$blk_id)
  months = unique(lubridate::month(sub.data$bs_yr_mon))
  list = list()

  for (i in 1L:12){
    list[[i]] = sub.data %>%
      dplyr::group_by(blk_id) %>%
      dplyr::filter(lubridate::month(bs_yr_mon) == months[i]) %>%
      dplyr::summarise(mean(avg_use_amt_m12))
  }

  output = purrr::reduce(list, dplyr::full_join, by = "blk_id")
  pi = apply(output[, 2:13], 1L, function(x) (log(x) - log(lag(x))) * 100)
  rows = paste("t", 1L:12, sep = "")
  rownames(pi) = rows
  colnames(pi) = blk_id

  return(pi)
}

cci.elasticity.cts = function(data, target = c("ADM_DONG", "ZIP_CD")){

  data = kcb.check(data)

  sub.data = data %>%
    dplyr::filter(gb == target) %>%
    na.omit()

    blk_id = unique(sub.data$blk_id)
    list = list()

    for (i in 1L:length(unique(sub.data$blk_id))){
      list[[i]] = sub.data[sub.data$blk_id == blk_id[i], ]
    }

    lm.list = list()
    coef.list = list()

    # List of linear regression to calculate delta C / delta S

    for (i in 1L:length(list)){
      lm.list[[i]] = lm(avg_use_amt_m12 ~ deposit_amt, data = list[[i]])
    }
    for (j in 1L:length(list)){
      coef.list[[j]] = coef(lm.list[[j]][1L])[2]
    }

    coefs = as.numeric(unlist(coef.list))

    s.over.c.median = sub.data %>%
      dplyr::group_by(blk_id) %>%
      dplyr::mutate(s.over.c = deposit_amt / avg_use_amt_m12) %>%
      dplyr::summarise(median(s.over.c)) %>%
      dplyr::select(-blk_id)

    ela.consumpt.savings = round(coefs * s.over.c.median, 3)
    output = as_tibble(cbind(blk_id, ela.consumpt.savings))

  return(output)
}

cci.elasticity.ctd = function(data, target = c("ADM_DONG", "ZIP_CD")){

  data = kcb.check(data)

  sub.data = data %>%
    dplyr::filter(gb == target) %>%
    na.omit()

  blk_id = unique(sub.data$blk_id)
  list = list()

  for (i in 1L:length(unique(sub.data$blk_id))){
    list[[i]] = sub.data[sub.data$blk_id == blk_id[i], ]
  }

  lm.list = list()
  coef.list = list()

  # List of linear regression to calculate delta C / delta S

  for (i in 1L:length(list)){
    lm.list[[i]] = lm(avg_use_amt_m12 ~ avg_bal_amt, data = list[[i]])
  }
  for (j in 1L:length(list)){
    coef.list[[j]] = coef(lm.list[[j]][1L])[2]
  }

  coefs = as.numeric(unlist(coef.list))

  d.over.c.median = sub.data %>%
    dplyr::group_by(blk_id) %>%
    dplyr::mutate(d.over.c = avg_bal_amt / avg_use_amt_m12) %>%
    dplyr::summarise(median(d.over.c)) %>%
    dplyr::select(-blk_id)

  ela.consumpt.debt = round(coefs * d.over.c.median, 3)
  output = as_tibble(cbind(blk_id, ela.consumpt.debt))

  return(output)
}

cci.elasticity.itc = function(data, target = c("ADM_DONG", "ZIP_CD")){

  data = kcb.check(data)

  sub.data = data %>%
    dplyr::filter(gb == target) %>%
    na.omit()

  blk_id = unique(sub.data$blk_id)
  list = list()

  for (i in 1L:length(unique(sub.data$blk_id))){
    list[[i]] = sub.data[sub.data$blk_id == blk_id[i], ]
  }

  lm.list = list()
  coef.list = list()

  # List of linear regression to calculate delta C / delta S

  for (i in 1L:length(list)){
    lm.list[[i]] = lm(avg_use_amt_m12 ~ med_icm, data = list[[i]])
  }
  for (j in 1L:length(list)){
    coef.list[[j]] = coef(lm.list[[j]][1L])[2]
  }

  coefs = as.numeric(unlist(coef.list))

  i.over.c.median = sub.data %>%
    dplyr::group_by(blk_id) %>%
    dplyr::mutate(i.over.c = med_icm / avg_use_amt_m12) %>%
    dplyr::summarise(median(i.over.c)) %>%
    dplyr::select(-blk_id)

  ela.consumpt.icm = round(coefs * i.over.c.median, 3)
  output = as_tibble(cbind(blk_id, ela.consumpt.icm))

  return(output)
}

ineq.icm.within = function(data, target = c("ADM_DONG", "ZIP_CD")){

  data = kcb.check(data)

  sub.data = data %>%
    dplyr::filter(gb == target)

  G = function (x, corr = FALSE, na.rm = TRUE)
  {
    if (!na.rm && any(is.na(x)))
      return(NA_real_)
    x = as.numeric(na.omit(x))
    n = length(x)
    x = sort(x)
    G = sum(x * 1L:n)
    G = 2 * G/sum(x) - (n + 1L)

    if (corr)
      G/(n - 1L)
    else G/n
  }
  gini.index = sub.data %>%
    dplyr::group_by(blk_id) %>%
    dplyr::mutate(gini = G(avg_icm)) %>%
    dplyr::summarise(unique(gini))

  return(gini.index)
}

ineq.icm.between = function(data, target = c("ADM_DONG", "ZIP_CD")){

  data = kcb.check(data)

  sub.data = data %>%
    dplyr::filter(gb == target)

  G = function (x, corr = FALSE, na.rm = TRUE)
  {
    if (!na.rm && any(is.na(x)))
      return(NA_real_)
    x = as.numeric(na.omit(x))
    n = length(x)
    x = sort(x)
    G = sum(x * 1L:n)
    G = 2 * G/sum(x) - (n + 1L)

    if (corr)
      G/(n - 1L)
    else G/n
  }

  gini.index = sub.data %>%
    dplyr::mutate(gini = G(avg_icm)) %>%
    dplyr::summarise(unique(gini))

  return(gini.index)

}

ineq.bal.within = function(data, target = c("ADM_DONG", "ZIP_CD")){

  data = kcb.check(data)

  sub.data = data %>%
    dplyr::filter(gb == target)

  G = function (x, corr = FALSE, na.rm = TRUE)
  {
    if (!na.rm && any(is.na(x)))
      return(NA_real_)
    x = as.numeric(na.omit(x))
    n = length(x)
    x = sort(x)
    G = sum(x * 1L:n)
    G = 2 * G/sum(x) - (n + 1L)

    if (corr)
      G/(n - 1L)
    else G/n
  }

  gini.index = sub.data %>%
    dplyr::group_by(blk_id) %>%
    dplyr::mutate(gini = G(avg_bal_amt)) %>%
    dplyr::summarise(unique(gini))

  return(gini.index)
}

ineq.consumpt.within = function(data, target = c("ADM_DONG", "GR1L00M", "ZIP_CD")){

  data = kcb.check(data)

  G = function (x, corr = FALSE, na.rm = TRUE)
  {
    if (!na.rm && any(is.na(x)))
      return(NA_real_)
    x = as.numeric(na.omit(x))
    n = length(x)
    x = sort(x)
    G = sum(x * 1L:n)
    G = 2 * G/sum(x) - (n + 1L)

    if (corr)
      G/(n - 1L)
    else G/n
  }

  if (target == "ADM_DONG"){

    sub.data = data %>%
      dplyr::filter(gb == "ADM_DONG")

    gini.index = sub.data %>%
      dplyr::group_by(blk_id) %>%
      dplyr::mutate(gini = G(avg_use_amt_m12)) %>%
      dplyr::summarise(unique(gini))

    return(gini.index)
  }

  if (target == "GR100M"){

    sub.data = data %>%
      dplyr::filter(gb == "GR100M")

    gini.index = sub.data %>%
      dplyr::group_by(blk_id) %>%
      dplyr::mutate(gini = G(avg_use_amt_m12)) %>%
      dplyr::summarise(unique(gini))

    return(gini.index)
  }

  if (target == "ZIP_CD"){

    sub.data = data %>%
      dplyr::filter(gb == "ZIP_CD")

    gini.index = sub.data %>%
      dplyr::group_by(blk_id) %>%
      dplyr::mutate(gini = G(avg_use_amt_m12)) %>%
      dplyr::summarise(unique(gini))

    return(gini.index)
  }

  else{
    stop("Emergency-Stop!")
  }
}

self.gprofit = function(data){

  data = kcb.check(repr)

  months = unique(lubridate::month(data$st_yr_mon))
  sic = unique(data$sic_clsfy)

  rvn.list = list()
  icm.list = list()

  for (i in 1:length(months)){
    rvn.list[[i]] = data %>%
      dplyr::group_by(sic_clsfy) %>%
      dplyr::filter(lubridate::month(st_yr_mon) == months[i]) %>%
      dplyr::summarise(median(avg_rvn))
  }

  for (i in 1:length(months)){
    icm.list[[i]] = data %>%
      dplyr::group_by(sic_clsfy) %>%
      dplyr::filter(lubridate::month(st_yr_mon) == months[i]) %>%
      dplyr::summarise(median(avg_icm))
  }

  output.rvn = purrr::reduce(rvn.list, full_join, by = "sic_clsfy")
  output.icm = purrr::reduce(icm.list, full_join, by = "sic_clsfy")

  cols = paste("t", 1L:12, sep = "")
  cols = append(cols, "sic_clsfy", 0)
  output = output.rvn[, 2:13] / output.icm[, 2:13]
  results = as_tibble(cbind(sic, output))
  rownames(results) = sic
  colnames(results) = cols

  return(results)
}

self.vol.rvn = function(data){

  data = kcb.check(data)

  months = unique(lubridate::month(data$st_yr_mon))
  sic = unique(data$sic_clsfy)

  list = list()

  for (i in 1L:length(months)){
    list[[i]] = data %>%
      dplyr::group_by(sic_clsfy) %>%
      dplyr::filter(lubridate::month(st_yr_mon) == months[i]) %>%
      dplyr::summarise(median(avg_rvn))
  }

  output = purrr::reduce(list, full_join, by = "sic_clsfy")

  cols = paste("t", 1L:12, sep = "")
  cols = append(cols, "sic_clsfy", 0)
  colnames(output) = cols

  # Convert it to log returns and handling

  pi = apply(output[, 2:13], 1L, FUN = function(x) (log(x) - log(lag(x))) * 100)
  vol = apply(pi, 2, function(x) sd(x, na.rm = TRUE))

  results = as_tibble(cbind(sic, vol))

  return(results)
}

self.var.rvn = function(data){

  data = kcb.check(data)

  months = unique(lubridate::month(data$st_yr_mon))
  sic = unique(data$sic_clsfy)
  list = list()

  for (i in 1:length(months)){
    list[[i]] = data %>%
      dplyr::group_by(sic_clsfy) %>%
      dplyr::filter(lubridate::month(st_yr_mon) == months[i]) %>%
      dplyr::summarise(median(avg_rvn))
     }
  output = purrr::reduce(list, dplyr::full_join, by = "sic_clsfy")
  cols = paste("t", 1L:12, sep = "")
  cols = append(cols, "sic_clsfy", 0)
  colnames(output) = cols

  'convert to returns'
  returns = apply(output[, 2:(length(months) + 1L)], 1L, function(x) (log(x) - log(lag(x)))*100)

  if (sum(is.na(returns))){
    returns[is.na(returns)] = 0
  }

  var = apply(returns, 2, function(x) quantile(x, 0.01))
  results = as_tibble(cbind(sic, var))
  colnames(results) = c("sic_clsfy", "ValueAtRisk")
  return(results)
}

self.drawdown.rvn = function(data){

  # Window is set to 3, which is previous 3 months
  # This is to estimate maximum losses 3 months intervals
  # However, not using a complicated algorithm for now since only 12 months
  # are provided

  data = kcb.check(repr)

  cmax = function(x, windows){
    empty = rep(NA, length(x))
    for (i in 1L:length(x) - windows)
    {
      empty[i] = (x[i] - min(x[i : i+windows])) / min(x[i : i+windows])
    }
    return(empty)
  }

  months = unique(lubridate::month(data$st_yr_mon))
  sic = unique(data$sic_clsfy)
  list = list()

  for (i in 1:length(months)){
    list[[i]] = data %>%
      dplyr::group_by(sic_clsfy) %>%
      dplyr::filter(lubridate::month(st_yr_mon) == months[i]) %>%
      dplyr::summarise(median(med_rvn))
  }

  output = purrr::reduce(list, full_join, by = "sic_clsfy")

  cols = paste("t", 1L:12, sep = "")
  cols = append(cols, "sic_clsfy", 0)
  colnames(output) = cols

  partial_ret = apply(output[, 2:13], 1, function(x) x - lag(x))
  drawdown12 = apply(partial_ret, 2, function(x) min(x, na.rm = TRUE))

  results = as_tibble(cbind(sic, drawdown12))
  colnames(results) = c("sic_clsfy", "DROWDOWNS")

  return(results)
}

self.bankruptcy = function(data = data){

  sd.nm = function(x){
    stats::sd(x, na.rm = TRUE)
  }

  data = kcb.check(repr)

  months = unique(lubridate::month(data$st_yr_mon))
  sic = unique(data$sic_clsfy)

  list = list()

  for (i in 1L:length(months)){
    list[[i]] = data %>%
      dplyr::group_by(sic_clsfy) %>%
      dplyr::filter(lubridate::month(st_yr_mon) == months[i]) %>%
      dplyr::summarise(median(avg_rvn))
  }

  output = purrr::reduce(list, full_join, by = "sic_clsfy")

  cols = paste("t", 1L:12, sep = "")
  cols = append(cols, "sic_clsfy", 0)
  colnames(output) = cols
  rvn = output

  for (i in 1L:length(months)){
    list[[i]] = data %>%
      dplyr::group_by(sic_clsfy) %>%
      dplyr::filter(lubridate::month(st_yr_mon) == months[i]) %>%
      dplyr::summarise(median(avg_ln_bal))
  }

  output = purrr::reduce(list, full_join, by = "sic_clsfy")

  cols = paste("t", 1L:12, sep = "")
  cols = append(cols, "sic_clsfy", 0)
  colnames(output) = cols
  bal = output
  # Capital structure
  # RVN can be considered as equity

  mat.rvn = as.matrix(rvn[, -1L])
  mat.bal = as.matrix(bal[, -1L])

  Va = mat.rvn + mat.bal
  L = mat.bal[, 1L]
  V0 = Va[, 1L]

  L[is.na(L)] = 0
  Va[Va < 0] = 0
  Va[is.na(Va)] = 0

  sigma = apply(Va, 1L, function(x) sd.nm((log(x) - log(lag(x)))))

  for (i in 1L:length(sic)){
    list[[i]] = CreditRisk::Merton(L = L[i],
                                   V0 = V0[i], sigma = sigma[i], r = 0.03,
                                   t = seq(0, 1.2, 0.1))
  }

  names(list) = sic
  return(list)
}

# Diagnosis indexes

diag.hld.icm = function(data, target = c("ADM_DONG", "GR1L00M", "ZIP_CD")){

  data = kcb.check(data)

  # ranking all income and debts. the more index is higher, the
  # mode riskier.

  # taking monthly of all data in order to rank.

  if (target == "ADM_DONG"){

    sub.data = data %>%
      dplyr::filter(gb == "ADM_DONG")

    blk_id = unique(sub.data$blk_id)
    months = 1L:length(unique(lubridate::month(sub.data$bs_yr_mon)))

    list = list()
    for (i in 1:length(months)){
      list[[i]] = sub.data[lubridate::month(sub.data$bs_yr_mon) == months[i], ]
    }

    new_list = list()
    for (i in 1:length(months)){
      new_list[[i]] = list[[i]] %>%
        dplyr::mutate(icm_rank  = rank(-avg_icm)) %>%
        dplyr::mutate(bal_rank = rank(avg_bal_amt)) %>%
        dplyr::mutate(rank = bal_rank / icm_rank)
    }

    rank = list()
    for (i in 1:length(months)){
      rank[[i]] = new_list[[i]] %>%
        dplyr::group_by(blk_id) %>%
        dplyr::summarise(median(rank))
    }

    output = purrr::reduce(rank, dplyr::full_join, by = "blk_id")
    colnames = paste("t", 1L:12, sep = "")
    colnames = append(colnames, "blk_id", 0)
    colnames(output) = colnames

    # volatility
    mean = apply(output[, -1L], 1L, function(x) mean(x))
    vol = apply(output[, -1L], 1L, function(x) sd(x))
    interval = mean + 1.96 * sqrt(vol) / sqrt(12)

    # Jan to Dec, vol is the volatility of ranking over 12 months
    spatial_unity = as_tibble(cbind(blk_id, interval))

    # filtering 5 most dangerous administration divisions'
    max_5 = spatial_unity %>%
      dplyr::slice_max(interval, n = 5)
    blk_id_at_risk = as.character(max_5$blk_id)

    list = list()
    for (i in 1:length(blk_id_at_risk)){
      list[[i]] = sub.data[sub.data$blk_id == blk_id_at_risk[i], ]
    }

    dti.list = list()
    for (i in 1:length(blk_id_at_risk)){
      dti.list[[i]] = list[[i]] %>%
        dplyr::mutate(dti = list[[i]]$avg_bal_amt / list[[i]]$avg_icm) %>%
        dplyr::summarise(dti_mean = median(dti),
                         icm = median(avg_icm),
                         bal = median(avg_bal_amt),
                         dlq90_cnt = sum(dlq90_cnt))
    }
    output = purrr::reduce(dti.list, dplyr::full_join, by = c("dti_mean",
                                                              "icm",
                                                              "bal",
                                                              "dlq90_cnt"))
    rownames(output) = blk_id_at_risk
    colnames(output) = c("DTI", "ICM", "BAL", "DLQ90")

    return(output)
  }

  if (target == "GR100M"){

    sub.data = data %>%
      dplyr::filter(gb == "GR100M")

    blk_id = unique(sub.data$blk_id)
    months = 1L:length(unique(lubridate::month(sub.data$bs_yr_mon)))

    list = list()
    for (i in 1:length(months)){
      list[[i]] = sub.data[lubridate::month(sub.data$bs_yr_mon) == months[i], ]
    }

    new_list = list()
    for (i in 1:length(months)){
      new_list[[i]] = list[[i]] %>%
        dplyr::mutate(icm_rank  = rank(-avg_icm)) %>%
        dplyr::mutate(bal_rank = rank(avg_bal_amt)) %>%
        dplyr::mutate(rank = bal_rank / icm_rank)
    }

    rank = list()
    for (i in 1:length(months)){
      rank[[i]] = new_list[[i]] %>%
        dplyr::group_by(blk_id) %>%
        dplyr::summarise(median(rank))
    }

    output = purrr::reduce(rank, dplyr::full_join, by = "blk_id")
    colnames = paste("t", 1L:12, sep = "")
    colnames = append(colnames, "blk_id", 0)
    colnames(output) = colnames

    # volatility
    mean = apply(output[, -1L], 1L, function(x) mean(x))
    vol = apply(output[, -1L], 1L, function(x) sd(x))
    interval = mean + 1.96 * sqrt(vol) / sqrt(12)

    # Jan to Dec, vol is the volatility of ranking over 12 months
    spatial_unity = as_tibble(cbind(blk_id, interval))

    # filtering 5 most dangerous administration divisions'
    max_5 = spatial_unity %>%
      dplyr::slice_max(interval, n = 5)
    blk_id_at_risk = as.character(max_5$blk_id)

    list = list()
    for (i in 1:length(blk_id_at_risk)){
      list[[i]] = sub.data[sub.data$blk_id == blk_id_at_risk[i], ]
    }

    dti.list = list()
    for (i in 1:length(blk_id_at_risk)){
      dti.list[[i]] = list[[i]] %>%
        dplyr::mutate(dti = list[[i]]$avg_bal_amt / list[[i]]$avg_icm) %>%
        dplyr::summarise(dti_mean = median(dti),
                         icm = median(avg_icm),
                         bal = median(avg_bal_amt),
                         dlq90_cnt = sum(dlq90_cnt))
    }
    output = purrr::reduce(dti.list, dplyr::full_join, by = c("dti_mean",
                                                              "icm",
                                                              "bal",
                                                              "dlq90_cnt"))
    rownames(output) = blk_id_at_risk
    colnames(output) = c("DTI", "ICM", "BAL", "DLQ90")

    return(output)
  }

  if (target == "ZIP_CD"){

    sub.data = data %>%
      dplyr::filter(gb == "ZIP_CD")

    blk_id = unique(sub.data$blk_id)
    months = 1L:length(unique(lubridate::month(sub.data$bs_yr_mon)))

    list = list()
    for (i in 1:length(months)){
      list[[i]] = sub.data[lubridate::month(sub.data$bs_yr_mon) == months[i], ]
    }

    new_list = list()
    for (i in 1:length(months)){
      new_list[[i]] = list[[i]] %>%
        dplyr::mutate(icm_rank  = rank(-avg_icm)) %>%
        dplyr::mutate(bal_rank = rank(avg_bal_amt)) %>%
        dplyr::mutate(rank = bal_rank / icm_rank)
    }

    rank = list()
    for (i in 1:length(months)){
      rank[[i]] = new_list[[i]] %>%
        dplyr::group_by(blk_id) %>%
        dplyr::summarise(median(rank))
    }

    output = purrr::reduce(rank, dplyr::full_join, by = "blk_id")
    colnames = paste("t", 1L:12, sep = "")
    colnames = append(colnames, "blk_id", 0)
    colnames(output) = colnames

    # volatility
    mean = apply(output[, -1L], 1L, function(x) mean(x))
    vol = apply(output[, -1L], 1L, function(x) sd(x))
    interval = mean + 1.96 * sqrt(vol) / sqrt(12)

    # Jan to Dec, vol is the volatility of ranking over 12 months
    spatial_unity = as_tibble(cbind(blk_id, interval))

    # filtering 5 most dangerous administration divisions'
    max_5 = spatial_unity %>%
      dplyr::slice_max(interval, n = 5)
    blk_id_at_risk = as.character(max_5$blk_id)

    list = list()
    for (i in 1:length(blk_id_at_risk)){
      list[[i]] = sub.data[sub.data$blk_id == blk_id_at_risk[i], ]
    }

    dti.list = list()
    for (i in 1:length(blk_id_at_risk)){
      dti.list[[i]] = list[[i]] %>%
        dplyr::mutate(dti = list[[i]]$avg_bal_amt / list[[i]]$avg_icm) %>%
        dplyr::summarise(dti_mean = median(dti),
                         icm = median(avg_icm),
                         bal = median(avg_bal_amt),
                         dlq90_cnt = sum(dlq90_cnt))
    }
    output = purrr::reduce(dti.list, dplyr::full_join, by = c("dti_mean",
                                                              "icm",
                                                              "bal",
                                                              "dlq90_cnt"))
    rownames(output) = blk_id_at_risk
    colnames(output) = c("DTI", "ICM", "BAL", "DLQ90")

    return(output)
  }
}

diag.hld.credit = function(data){

  data = kcb.check(data)

  min.max.scaler = function(x){
    return((x - min(x))/ (max(x) - min(x)))
  }

  set.seed(Sys.time())
  # filtering necessary components to reduce computational stress
  # ADM_DONG only

  sub.data = data %>%
    dplyr::filter(gb == "ADM_DONG") %>%
    dplyr::mutate(dti = avg_bal_amt / avg_icm)

  blk_id = sub.data$blk_id
  nums = unlist(lapply(sub.data, is.numeric))
  num.cols = unlist(lapply(sub.data, is.numeric))
  subset = sub.data[, num.cols]

  # this horrible passive removing variable is to accelerate
  # BMA calculation.

  tmp = cor(subset, method = "spearman")
  tmp[upper.tri(tmp)] = 0
  diag(tmp) = 0

  # to avoid singular system, we remove variables exceeding 0.7

  cor.new = subset[, !apply(tmp, 2, function(x) any(abs(x) > 0.7, na.rm = TRUE))]

  # max linear regression slope selecting to determine threshold

  thresholds = function(){

    list = list()
    lm.list = list()

    seq = seq(0, 1L, 0.1)

    for (i in 1L:length(seq)){
      list[[i]] = subset %>%
        dplyr::filter(dti > quantile(dti, seq[i]) & dti <= quantile(dti, seq[i + 1L]))
    }

    for (i in 1L:length(seq)){
      lm.list[[i]] = cov(list[[i]]$dlq90_cnt, list[[i]]$dti) / var(list[[i]]$dlq90_cnt)
    }

    output = unlist(lm.list)
    index = index(max(output))

    return(index)
  }
  cor.new$dti[cor.new$dti == 0] = NA
  subset.final = na.omit(cor.new)

  # need to define danger threshold points. DLQ and DTI

  selected_columns = colnames(subset.final)

  s_subset = sub.data[selected_columns]

  # Applying 2nd filter BMA for 2nd one
  # New approach

  cols = c("gender_cd", "age_cd", "own_hous_rt", "avg_icm",
           "avg_use_amt_m12", "deposit_amt", "avg_bal_amt",
           "dlq90_cnt", "re_debt_amt")

  bmaval = sub.data[cols]

  bma.fit = BMA::bic.glm(dlq90_cnt ~.,
                         data = bmaval,
                         glm.family = "gaussian")

  label = bma.fit$label[1L]
  label = gsub(".x", "", as.character(label))
  label = unlist(strsplit(label, split = ","))
  bma_selected = sub.data[label]
  dlq90_cnt = sub.data$dlq90_cnt
  subset = cbind(dlq90_cnt, bma_selected)

  lm.fit = lm(dlq90_cnt ~., data = subset)

  coefs = coef(lm.fit)
  intercept = rep(1, dim(bma_selected)[1])
  selected = cbind(intercept, bma_selected)
  weighted = as.numeric(as.matrix(t(coefs)) %*% as.matrix(t(selected)))
  sub.data2 = cbind(sub.data, weighted)

  # Score System, dividing as 10
  seq = seq(0.1, 1, 0.1)
  points = list()

  for (i in 1:10){
    points[[i]] = quantile(sub.data2$weighted, seq[i])
  }

  point = unlist(points)
  score = list()

  for (i in 1:10){
    score[[i]] = sub.data2 %>%
      dplyr::filter(weighted > point[i] & weighted < point[i + 1]) %>%
      dplyr::select(dti, weighted)
  }

  scores = seq(paste0("score_", 1:10, sep = ""))

  names(score) = scores

  return(score)
}

diag.hld.fore.eco = function(data, target = c("ADM_DONG", "ZIP_CD")){

  'kcb data checker'
  data = kcb.check(data)

  min.max.scaler = function(x){
    return((x - min(x))/ (max(x) - min(x)))
  }

  'omitting NA values for scoring'

  sub.data = data %>%
    dplyr::filter(gb == "ADM_DONG") %>%
    dplyr::mutate(y.over.c = avg_use_amt_m12 / avg_icm) %>%
    dplyr::mutate(d.over.c = avg_bal_amt / avg_use_amt_m12)

  blk_id = unique(sub.data$blk_id)
  months = unique(lubridate::month(sub.data$bs_yr_mon))

  c.list = list()
  s.list = list()

  for (i in 1L:length(months)){
    c.list[[i]] = sub.data %>%
      dplyr::group_by(blk_id) %>%
      dplyr::filter(lubridate::month(bs_yr_mon) == months[i]) %>%
      dplyr::summarise(median(avg_use_amt_m12))
  }

  for (i in 1L:length(months)){
    s.list[[i]] = sub.data %>%
      dplyr::group_by(blk_id) %>%
      dplyr::filter(lubridate::month(bs_yr_mon) == months[i]) %>%
      dplyr::mutate(savings = deposit_amt) %>%
      dplyr::summarise(median(savings))
  }

  c.output = purrr::reduce(c.list, dplyr::full_join, by = "blk_id")
  s.output = purrr::reduce(s.list, dplyr::full_join, by = "blk_id")
  ela.itc.list = list()

  for (i in 1L:length(unique(sub.data$blk_id))){
    ela.itc.list[[i]] = sub.data[sub.data$blk_id == blk_id[i], ]
  }

  lm.list = list()
  coef.list = list()

  for (i in 1L:length(blk_id)){
    lm.list[[i]] = lm(avg_icm ~ avg_use_amt_m12, data = ela.itc.list[[i]])
  }
  for (j in 1L:length(blk_id)){
    coef.list[[j]] = coef(lm.list[[j]][1L])[2]
  }

  c.coefs = as.numeric(unlist(coef.list))

  c.over.y.mean = sub.data %>%
    dplyr::group_by(blk_id) %>%
    dplyr::summarise(median(y.over.c)) %>%
    dplyr::select(-blk_id)

  ela.consumpt.income = round(c.coefs * c.over.y.mean, 3)

  pi = apply(c.output[, 2:(length(months) + 1)], 1L, function(x) (log(x) - log(lag(x))) * 100)
  pi[is.na(pi)] = 0

  columns = paste("t", 1L:12, sep = "")
  colnames(pi) = blk_id
  rownames(pi) = columns

  s = apply(s.output[, 2:(length(months) + 1)], 1L, function(x) (log(x) - log(lag(x))) * 100)
  s[is.na(s)] = 0

  vol = list()

  #rolling volatility
  for (i in 1:length(blk_id)){
    vol[[i]] =zoo::rollapply(s[, i], width = 2, FUN = sd)
  }

  vol.output = as.data.frame(purrr::reduce(vol, cbind))
  colnames(vol.output) = blk_id
  rows = paste("t", 2:12, sep = "")
  rownames(vol.output) = rows
  vol.output[is.na(vol.output)] = 0

  #removing first month on savings
  # if monthly difference is positive, CCI increases, therefore,
  # it is 1 if it is positive, it is 0, if it is negative.
  # for Saving differences, it is 1 when it is negative, and it is 0 when it is positive

  # Criteria, mainly Pi, if elasticity of income is greater than 1.
  Pcopy = pi
  Pbinary = apply(Pcopy, 2, function(x) base::ifelse(x > 0, 1, 0))

  mpbinary = apply(Pbinary, 1, mean)
  volatility = apply(vol.output, 1, median)
  Svol = append(volatility, 0, 0)

  # Graphic
  stats::ts.plot(min.max.scaler(Svol),
                 type = "b",
                 col = "blue",
                 ylab = "")
  lines(min.max.scaler(mpbinary),
        type = "b",
        col = "red")
  legend("bottomright",
         legend = c("CCI", "Uncertain"),
         col = c("red", "blue"),
         lwd = 2, cex = 0.7)

  return(mpbinary)
}

diag.cci.self.rvn = function(data, repr){

  data = kcb.check(data)
  repr = kcb.check(repr)

  sub.data = data %>%
    dplyr::filter(gb == "ADM_DONG")

  cd_code = unique(sub.data$blk_id)

  dates = unique(sub.data$bs_yr_mon)
  months = unique(lubridate::month(dates))

  list = list()

  'Consumption inflation, icm increase can lead to consumption increase'
  for (i in 1L:12){
    list[[i]] = sub.data %>%
      dplyr::group_by(blk_id) %>%
      dplyr::filter(lubridate::month(bs_yr_mon) == months[i]) %>%
      dplyr::summarise(sum(avg_use_amt_m12))
  }

  output.civil = purrr::reduce(list, dplyr::full_join, by = "blk_id")
  mat = as.matrix(output.civil[, -1L])
  mat.pi = as.matrix(apply(mat, 1L, function(x) (log(x) - log(lag(x)))))

  columns = paste("t", 1L:12, sep = "")
  rownames(mat.pi) = columns
  colnames(mat.pi) = cd_code

  list = list()

  for (i in months){
    list[[i]] = repr %>%
      dplyr::group_by(adm_dong_cd) %>%
      dplyr::filter(lubridate::month(st_yr_mon) == months[i]) %>%
      dplyr::summarise(mean(avg_rvn))
  }

  output = purrr::reduce(list, dplyr::full_join, by = "adm_dong_cd")
  # correlation between RVN and inflation
  mat.repr = output[, -1L]
  colnames = paste("t", 1L:12, sep = "")
  mat.repr = as.matrix(mat.repr)
  mat.pi[is.na(mat.pi)] = 0
  t.mat.pi = t(mat.pi)

  'convert as % changes'
  d.mat.repr = apply(mat.repr, 1L, function(x) (log(x) - log(lag(x))))
  d.mat.repr[is.na(d.mat.repr)] = 0
  t.mat.repr = t(d.mat.repr)

  cor = list()
  for (i in 1L:length(cd_code)){
    cor[[i]] = cor(t.mat.repr[i, ], t(mat.pi)[i, ], method = "spearman")
  }

  cor.mat = as.data.frame(unlist(cor))
  rownames(cor.mat) = cd_code
  colnames(cor.mat) = "cor"

  'rvn by sector'
  by_blk_id = list()
  sic_list = list()
  output_list = list()
  new_months = sprintf("%02d", 1L:12)

  for (i in 1L:length(cd_code)){
    by_blk_id[[i]] = repr[repr$adm_dong_cd == cd_code[i], ]
  }

  for (i in 1L:length(cd_code)){
    for (j in 1L:length(new_months)){
      sic_list[[j]] = by_blk_id[[i]] %>%
        dplyr::group_by(sic_clsfy) %>%
        dplyr::filter(st_mon == new_months[j]) %>%
        dplyr::summarise(mean(avg_rvn))
        output_list[[i]] = purrr::reduce(sic_list, dplyr::full_join, by = "sic_clsfy")[, 1L:13]
    }
  }

  'applying 0 to all Na values in every districts'
  for (i in 1L:length(output_list)){
    output_list[[i]][is.na(output_list[[i]])] = 0
  }

  sic_clsfy = list()
  rvn_list = list()
  returns = list()

  'conserve sic_clsfy'
  for (i in 1L:length(output_list)){
    sic_clsfy[[i]] = output_list[[i]][, 1L]
    rvn_list[[i]] = output_list[[i]][, 2:(length(new_months) + 1L)]
  }

  for (i in 1L:length(output_list)){
    returns[[i]] = apply(rvn_list[[i]], 1L, function(x) log(x) - log(lag(x)))
  }

  for (i in 1L:length(returns)){
    returns[[i]][is.infinite(returns[[i]])] = 0
    returns[[i]][is.na(returns[[i]])] = 0
  }

  output = list()

  for (i in 1L:length(returns)){
    output[[i]] = apply(returns[[i]], 2, function(x) (prod(1L + x))^(1L/12) - 1L)
  }

  results = list()
  for (i in 1L:length(returns)){
    results[[i]] = cbind(sic_clsfy[[i]], output[[i]])
  }

  return(results)
}

diag.self.risk = function(data){

  'data checker'
  data = kcb.check(data)

  'na.rm sd function'
  na.rm.sd = function(x){
    return(sd(x, na.rm = TRUE))
  }

  'na.rm mean function'
  na.mean = function(x){
    return(mean(x, na.rm = TRUE))
  }

  'geometric return calculator function'
  geo.ret = function(x){
    return((prod(1L + x))^(1L/12) - 1L)
  }

  'drawdown function'
  cmax = function(x, windows){
    empty = rep(NA, length(x))
    for (i in 1L:length(x) - windows)
    {
      empty[i] = (x[i] - min(x[i : i+windows])) / min(x[i : i+windows])
    }
    return(empty)
  }

  'minimum handling NA value'
  min.rm = function(x){
    return(min(x, na.rm = TRUE))
  }

  months = 1L:12
  sic = unique(data$sic_clsfy)

  list = list()
  data.frame.list = list()

  for (i in months){
    list[[i]] = data %>%
      dplyr::group_by(sic_clsfy) %>%
      dplyr::filter(lubridate::month(st_yr_mon) == months[i]) %>%
      dplyr::summarise(median(avg_rvn))
    output = purrr::reduce(list, dplyr::full_join, by = "sic_clsfy")
  }

  colnames = paste("t", 1L:12, sep = "")
  colnames = append(colnames, "sic_clsfy", 0)
  colnames(output) = colnames

  'convert to returns'
  returns = apply(output[, 2:(length(months) + 1L)], 1L, function(x) log(x) - log(lag(x)))

  if (sum(is.na(returns))){
    returns[is.na(returns)] = 0
  }
  colnames(returns) = sic

  'calculating geometric returns on sectors regardless of adm_dong,
  calculating means of the whole dataset'
  georet = as.data.frame(apply(returns, 2, geo.ret))
  mean = na.mean(georet$`apply(returns, 2, geo.ret)`)

  'volatility calculation'
  vol = apply(output[, 2:(length(months) + 1L)], 1L, function(x) na.rm.sd(log(x) - log(lag(x))))
  vol.df = as.data.frame(vol)
  rownames(vol.df) = sic

  df = cbind(georet, vol.df)
  colnames(df) = c("ret", "vol")

  vol.threshold = mean(na.omit(df$vol))

  self.at.risk = df %>%
    dplyr::filter(vol >= vol.threshold & ret <= 0)

  list = list()

  for (i in months){
    list[[i]] = data %>%
      dplyr::group_by(sic_clsfy) %>%
      dplyr::filter(lubridate::month(st_yr_mon) == months[i]) %>%
      dplyr::summarise(median(avg_rvn))
    output = purrr::reduce(list, dplyr::full_join, by = "sic_clsfy")
  }

  colnames = paste("t", 1L:12, sep = "")
  colnames = append(colnames, "sic_clsfy", 0)
  colnames(output) = colnames

  'convert to returns'
  returns = apply(output[, 2:(length(months) + 1L)], 1L, function(x) log(x) - log(lag(x)))

  if (sum(is.na(returns))){
    returns[is.na(returns)] = 0
  }
  colnames(returns) = sic

  var = apply(returns, 2, function(x) quantile(x, 0.01))

  sic = unique(data$sic_clsfy)

  mat = matrix(NA, length(sic), length(months))

  list = list()
  repr.list = list()

  for (i in months){
    list[[i]] = data %>%
      dplyr::group_by(sic_clsfy) %>%
      dplyr::filter(lubridate::month(st_yr_mon) == months[i]) %>%
      dplyr::summarise(mean(avg_rvn))
      repr.list[[i]] = as.data.frame(list[[i]])
  }

  repr = reduce(repr.list, full_join, by = "sic_clsfy")

  col.names = paste("t", 1L:12, sep = "")
  colnames = append(col.names, "sic_clsfy", 0)
  colnames(repr) = colnames

  risk.mat = apply(repr[, -1L], 1L, function(x) x - lag(x))
  losses = apply(risk.mat, 2, min.rm)
  results = as.data.frame(cbind(sic, round(losses, 0)))

  new.output = as.data.frame(cbind(var, results$V2))
  new.output$var = as.numeric(new.output$var)
  new.output$V2 = as.numeric(new.output$V2)

  d.class = rownames(self.at.risk)

  new.output_ = new.output %>%
    dplyr::mutate(var.rank = rank(var),
                  draw.rank = rank(V2))
  risky = new.output_[d.class, ]

  'minimum selection requirement'
  concat = 10
  while (concat <= 20) {
    sic.at.risk.final = risky %>%
      dplyr::filter(var.rank <= concat & draw.rank <= concat)

    if (dim(sic.at.risk.final)[1] <= 10){
      concat = concat + 1
    }
  }

  rows = rownames(sic.at.risk.final)
  colnames(sic.at.risk.final) = c("VaR", "drawdowns", "VaRRank", "DrawRank")
  return(sic.at.risk.final)
}


