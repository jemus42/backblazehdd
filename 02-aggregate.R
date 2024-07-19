library(data.table)

rds_files <- fs::dir_ls("data", glob = "data*rds")

# xtab <- readRDS(rds_files[[1]])

xtab <- rbindlist(lapply(rds_files, readRDS), fill = TRUE)
xtab <- xtab[order(serial_number, date)]

xtab[, is_legacy_format := fifelse(is.na(is_legacy_format), TRUE, is_legacy_format)]

drive_metadata <- unique(xtab[, .(serial_number, model, capacity_gib)])
setkey(drive_metadata, serial_number)

xtab[, let(model = NULL, capacity_gib = NULL)]

xtab <- xtab[ , .SD[unique(c(1, .N))], by = .(serial_number, failure)]
# xtab <- xtab[order(serial_number, date)]

xtab[serial_number == "WD-WCAWZ0495587"]


xsurv <- xtab[, .(
  t1 = first(date), 
  t2 = last(date), 
  n = .N, 
  failure = sum(failure)
), keyby = .(serial_number)]
xsurv[, days := t2 - t1]

xsurv

drive_metadata[xsurv]

table(xsurv$failure)

xsurv[n > 2][order(n)]

library(survival)

xsurv[, failure := fifelse(failure == 2, 1, failure)]

survfit(formula = Surv(days, failure) ~ 1, data = xsurv) 

