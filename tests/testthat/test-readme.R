context("README examples")

test_that("result from first dplyr example in README (with some tweaks) is consistent with result on tbl_df", {
  check_impala()
  test_op <- function(x) {
    x %>%
      select(tailnum, distance, arr_delay) %>%
      filter(!is.na(arr_delay)) %>%
      group_by(tailnum) %>%
      summarise(count = n(), dist = mean(distance), delay = mean(arr_delay)) %>%
      filter(count > 20L, dist < 2000L) %>%
      arrange(delay, dist, count, tailnum) %>%
      collect() %>%
      mutate_if(is.double, round, 2)
  }
  compare_tbls(list(tbl(impala, "flights"), nycflights13::flights), op = test_op, convert = TRUE)
})

test_that("result from second dplyr example in README is consistent with result on tbl_df", {
  check_impala()
  test_op <- function(x) {
    x %>%
      transmute(flight_code = paste0(carrier, as.character(flight))) %>%
      distinct(flight_code)
  }
  compare_tbls(list(tbl(impala, "flights"), nycflights13::flights), op = test_op)
})

test_that("result from third dplyr example in README (with some tweaks) is consistent with result on tbl_df", {
  check_impala()
  test_op <- function(x) {
    x %>%
      filter(!is.na(arr_delay)) %>%
      group_by(year, month, day) %>%
      filter(arr_delay == max(arr_delay)) %>%
      arrange(year, month, day, carrier, flight) %>%
      collect() %>%
      select(-time_hour) %>%
      mutate_if(is.double, round, 2)
  }
  compare_tbls(list(tbl(impala, "flights"), nycflights13::flights), op = test_op, convert = TRUE)
})

test_that("result from fourth dplyr example in README (with some tweaks) is consistent with result on tbl_df", {
  check_impala()
  test_op <- function(x, y) {
    x %>% inner_join(y, by = "carrier") %>%
      arrange(year, month, day, carrier, flight) %>%
      head(1000) %>%
      collect() %>%
      select(-time_hour)
  }
  compare_tbls2(
    list(tbl(impala, "flights"), nycflights13::flights),
    list(tbl(impala, "airlines"), nycflights13::airlines),
    op = test_op,
    convert = TRUE
  )
})

test_that("result from fifth dplyr example in README (with some tweaks) is consistent with result on tbl_df", {
  check_impala()
  test_op <- function(x, y) {
    y <- y %>% filter(name == "Southwest Airlines Co.")
    x %>%
      semi_join(y, by = "carrier") %>%
      arrange(year, month, day, carrier, flight) %>%
      head(1000) %>%
      collect() %>%
      select(-time_hour)
  }
  compare_tbls2(
    list(tbl(impala, "flights"), nycflights13::flights),
    list(tbl(impala, "airlines"), nycflights13::airlines),
    op = test_op,
    convert = TRUE
  )
})

test_that("result from first SQL example in README succeeds", {
  check_impala()
  dbExecute(impala, "REFRESH flights")
  succeed()
})

test_that("result from second SQL example in README succeeds", {
  check_impala()
  flights_by_carrier_df <- dbGetQuery(
    impala,
    "SELECT carrier, COUNT(*) FROM flights GROUP BY carrier"
  )
  succeed()
})

test_that("result from third SQL example in README (with some tweaks) succeeds", {
  check_impala()
  flights_tbl <- tbl(impala, sql("SELECT * FROM flights LIMIT 10"))
  succeed()
})
