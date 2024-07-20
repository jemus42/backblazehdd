# https://www.backblaze.com/cloud-storage/resources/hard-drive-test-data
# https://f001.backblazeb2.com/file/Backblaze-Hard-Drive-Data/data_Q1_2020.zip

library(data.table)

data_dir <- "data/rds"
usethis::use_directory(data_dir)

csvs <- fs::dir_ls("data-raw", glob = "*.csv", recurse = TRUE)


base_name <- fs::path_file(csvs) |>
  fs::path_ext_remove()

csvtab <- data.table(
  date = as.Date(base_name),
  csv = csvs,
  key = "date"
)

# Making it easier to qiickly filter
csvtab[, year := data.table::year(date)]
csvtab[, month := data.table::month(date)]

#' Read a single CSV (1 day)
read_single_day <- function(csv, keep_smart_raw = FALSE, keep_smart_normalized = FALSE) {
  # csv <- "data-raw/csv/2013-04-10.csv"
  xdat <- fread(csv, showProgress = FALSE)

  # Edge case: 2018-02-25.csv is a CR-terminated file where `date` is a different format
  if (inherits(xdat[["date"]], "character")) {
    # Remaining dates are parsed as IDate and IDateTime returns a two-element vector with idate and itime
    xdat[, date := data.table::IDateTime(as.Date(date, format = "%m/%d/%y"))[["idate"]]]
  }

  # Edge case: 2014-11-02.csv is empty and only consists of headers, so we have missingness on that day :/
  if (nrow(xdat) == 0) return(NULL)

  if (!keep_smart_raw) {
    which_raw <- endsWith(names(xdat), "raw")
    xdat <- xdat[, !(which_raw), with = FALSE]
  }

  if (!keep_smart_normalized) {
    which_normalized <- endsWith(names(xdat), "normalized")
    xdat <- xdat[, !(which_normalized), with = FALSE]
  }

  # Store capacity as Gibibytes to not need an i64 column
  xdat[, capacity_gib := capacity_bytes/1024^3]
  xdat[, capacity_bytes := NULL]

  xdat[]
}

#' Read many CSVs and reduce information to first/last observation by failure
#' 
#' There are also instances where a drive is marked as failure == 1 but on a later date the same drive has failure == 0,
#' so we also group by failure to retain that information and make a decision later. Example case: "WD-WCAWZ0495587"
#' @param csvs `character()` vector of CSV file paths.
#' @return A `data.table` with the same columns as produced by `read_single_day()` but retaining
#' only first and last observation per `serial_number`
reduce_csvs <- function(csvs, keep_smart_raw = FALSE, keep_smart_normalized = FALSE) {
  # csvs <- csvtab[year == 2013, csv]
  tablist <- future.apply::future_lapply(csvs, \(csv) {
    read_single_day(csv, keep_smart_raw = keep_smart_raw, keep_smart_normalized = keep_smart_normalized)
  })

  xtab <- rbindlist(tablist)
  xtab <- xtab[order(date, serial_number)]

  # Assuming data is ordered by date, we get the first and last (n-th) row per serial number
  # Since serials can appear only once we unique() to ensure c(1, 1) -> 1, to avoid duplicates
  xtab <- xtab[ , .SD[unique(c(1, .N))], by = .(serial_number, failure)]
  xtab[]
}

# Reduce data in parallel
future::plan("multisession", workers = getOption("Ncpus"))

yearmonth <- unique(csvtab[, .(year, month)])

for (i in seq_len(nrow(yearmonth))) {
  current_year <- yearmonth[i, year]
  current_month <- yearmonth[i, month]
  
  dest_file <- sprintf("data_%i-%02i.rds", current_year, current_month)
  dest_file <- fs::path(data_dir, dest_file)

  if (fs::file_exists(dest_file)) next

  cli::cli_alert_info("Reading data from {.value {current_year}} month {.value {current_month}}")

  current_csvs <- csvtab[year == current_year & month == current_month, csv]
  
  tictoc::tic()
  xtab <- reduce_csvs(current_csvs, keep_smart_raw = FALSE, keep_smart_normalized = TRUE)
  tictoc::toc()

  cli::cli_alert_info("Writing to {dest_file}")
  saveRDS(xtab, file = dest_file)
}
