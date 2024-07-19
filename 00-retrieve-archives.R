library(rvest)

# Timeout for `download.file()`, default is 60s und too short for large zip files.
options(timeout = max(300, getOption("timeout")))

# Ensure output directories exist
usethis::use_directory("data-raw")
usethis::use_directory("data-raw/zip")
usethis::use_directory("data-raw/csv")

index_url <- "https://www.backblaze.com/cloud-storage/resources/hard-drive-test-data"
index_html <- read_html(index_url) 

# Extracting all links and filtering for zips was easier than finding out
# why the specific selector for the <a> element did not work :/
zip_urls <- index_html |>
  html_nodes("a") |>
  html_attr(name = "href") |>
  stringr::str_subset(pattern = "Backblaze-Hard-Drive-Data\\/data_.*zip$") |>
  unique()

# The zip files end up being roughly 24GB in total as of July 2024.
cli::cli_progress_bar("Getting zip files", total = length(zip_urls))
for (zip in zip_urls) {
  cli::cli_progress_update()

  dest_file <- fs::path(here::here("data-raw/zip"), fs::path_file(zip))

  if (fs::file_exists(dest_file)) {
    cli::cli_alert_info("{.file {dest_file}} already exists, skipping...")
    next
  }

  download.file(zip, destfile = dest_file)

}
cli::cli_progress_done()

# Extract zips
zips <- fs::dir_ls("data-raw/zip", glob = "*zip")

# This will create a lot of CSV files, one per day, for over a decade.
# Don't flood your filesystem please.
# Note that years 2013 to 2015 will extract to a folder of the year which contains the CSV files,
# whereas subsequent archives are per quarter and directly extract all CSV files in the extraction directory.
# At least until 2022, then it's one subfolder per archive again.
purrr::walk(zips, \(x) {
  unzip(x, exdir = "data-raw/csv")
})
