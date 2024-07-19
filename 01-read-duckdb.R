library(dplyr)
library(duckplyr)
duckplyr::methods_overwrite()

Sys.setenv(DUCKPLYR_FALLBACK_COLLECT = 1)
Sys.setenv(DUCKPLYR_FALLBACK_AUTOUPLOAD = 1)

data_dir <- "data/parquet"
usethis::use_directory(data_dir)

# bench::mark(
#   duckplyr::df_from_csv("data-raw/csv/2013-04-10.csv"),
#   data.table::fread("data-raw/csv/2013-04-10.csv"),
#   read.csv("data-raw/csv/2013-04-10.csv"),
#   vroom::vroom("data-raw/csv/2013-04-10.csv"),
#   check = FALSE
# )

# duckplyr::duckplyr_df_from_csv("data-raw/csv/2013-04-10.csv")

collect_csvs <- function() {
  csvs <- fs::dir_ls("data-raw", glob = "*.csv", recurse = TRUE)

  base_name <- fs::path_file(csvs) |>
    fs::path_ext_remove()
  
  csvdt <- data.table::data.table(
    date = as.Date(base_name),
    csv = csvs,
    key = "date"
  )
  
  # Making it easier to qiickly filter
  csvdt[, year := data.table::year(date)]
  csvdt[, month := data.table::month(date)]
  csvdt[]
}

csvtab <- collect_csvs()

#' Read a single CSV (1 day)
read_single_day <- function(csv, keep_smart_raw = FALSE, keep_smart_normalized = FALSE) {
  # csv <- "data-raw/csv/2013-04-10.csv"
  # Would need to pre-specify type for dozens of SMART values which may or may not exist depending on date of data
  # ...so we manually convert afetrwards, which is slower I guess.
  xdat <- duckplyr::duckplyr_df_from_csv(csv)

  # Edge case: 2018-02-25.csv is a CR-terminated file where `date` is a different format
  # But duckdb does not seem to have an issue auto-detecting that
  if (!inherits(xdat[["date"]], "Date")) {
    cli::cli_alert_warning("Issue parsing date in {.file {csv}}")
    # Remaining dates are parsed as IDate and IDateTime returns a two-element vector with idate and itime
    # xdat[, date := data.table::IDateTime(as.Date(date, format = "%m/%d/%y"))[["idate"]]]
  }

  # Edge case: 2014-11-02.csv is empty and only consists of headers, so we have missingness on that day :/
  if (nrow(xdat) == 0) {
    cli::cli_alert_warning("File appears to be empty: {.file {csv}}")
    return(NULL)
  }

  if (!keep_smart_raw) {
    xdat <- xdat |>
      select(!matches("^smart.*raw$"))
  }

  if (!keep_smart_normalized) {
    xdat <- xdat |>
      select(!matches("^smart.*normalized$"))
  }
  
  if (keep_smart_raw | keep_smart_normalized) {
    xdat <- xdat |>
      mutate(across(matches("^smart"), as.integer))
  }

  # Store capacity as Gibibytes for compactness
  xdat |>
    mutate(
      failure = as.integer(failure),
      capacity_gib = capacity_bytes/1024^3, 
      .after = "model"
    ) |>
    select(-capacity_bytes)
}

future::plan("multisession", workers = getOption("Ncpus"))

#  csvs <- sample(csvtab$csv, 10)

#' Read many CSVs and reduce information to first/last observation by failure
#' 
#' There are also instances where a drive is marked as failure == 1 but on a later date the same drive has failure == 0,
#' so we also group by failure to retain that information and make a decision later. Example case: "WD-WCAWZ0495587"
#' @param csvs `character()` vector of CSV file paths.
#' @return A `duckplyr` df with the same columns as produced by `read_single_day()` but retaining
#' only first and last observation per `serial_number`
reduce_csvs <- function(csvs, keep_smart_raw = FALSE, keep_smart_normalized = FALSE) {
  # csvs <- csvtab[year == 2013, csv]
  tablist <- future.apply::future_lapply(csvs, \(csv) {
    read_single_day(csv, keep_smart_raw = keep_smart_raw, keep_smart_normalized = keep_smart_normalized)
  })

  xtab <- bind_rows(tablist) 

  # Assuming data is ordered by date, we get the first and last (n-th) row per serial number
  # Since serials can appear only once we unique() to ensure c(1, 1) -> 1, to avoid duplicates
  xtab |>
    arrange(date, serial_number) |>
    slice(c(1, n()), .by = c("serial_number", "failure"))
}

yearmonth <- unique(csvtab[, .(year, month)])

for (i in seq_len(nrow(yearmonth))) {
  current_year <- yearmonth[i, year]
  current_month <- yearmonth[i, month]
  
  dest_file <- sprintf("data_%i-%02i.parquet", current_year, current_month)
  dest_file <- fs::path(data_dir, dest_file)

  if (fs::file_exists(dest_file)) next
  cli::cli_alert_info("Reading data from {.value {current_year}} month {.value {current_month}}")

  current_csvs <- csvtab[year == current_year & month == current_month, csv]
  
  tictoc::tic()
  xtab <- reduce_csvs(current_csvs, keep_smart_raw = FALSE, keep_smart_normalized = TRUE)
  tictoc::toc()

  cli::cli_alert_info("Writing to {dest_file}")
  df_to_parquet(xtab, path = dest_file)
  rm(xtab)
}
