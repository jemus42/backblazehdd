library(duckplyr)
duckplyr::methods_overwrite()

parquet_files <- as.character(fs::dir_ls("data/parquet", glob = "data*parquet"))

xtab <- bind_rows(lapply(parquet_files, duckplyr_df_from_parquet))

drive_metadata <- xtab |>
  select(serial_number, model, capacity_gib) |>
  distinct()

xtab <- xtab |>
  arrange(date, serial_number) |>
  mutate(
    is_legacy_format = if_else(is.na(is_legacy_format), TRUE, is_legacy_format)
  )


xtab <- xtab |>
  arrange(date, serial_number) |>
  slice(c(1, n()), .by = c("serial_number", "failure"))


xtab[serial_number == "WD-WCAWZ0495587"]

xtab <- duckplyr::as_duckplyr_df(xtab)

xsurv <- xtab |>
  summarize(
    t1 = first(date), 
    t2 = last(date), 
    days = as.numeric(t2, t1, unit = "days"),
    n = n(), 
    failure = sum(failure),
    .by = "serial_number"
  )


table(xsurv$failure)
