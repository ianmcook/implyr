context("lubridate function translations")

library(lubridate)

test_that("year() returns expected result", {
  check_impala()
  test_op <- function(x) {
    x %>%
      transmute(extracted_year = year(time_hour)) %>%
      group_by(extracted_year) %>%
      summarise(num_flights = n()) %>%
      arrange(extracted_year) %>%
      collect() %>%
      transmute(num_flights = as.numeric(num_flights))
  }
  compare_tbls(list(tbl(impala, "flights"), nycflights13::flights), op = test_op)
})

test_that("month() returns expected result", {
  check_impala()
  test_op <- function(x) {
    x %>%
      transmute(extracted_month = month(time_hour)) %>%
      group_by(extracted_month) %>%
      summarise(num_flights = n()) %>%
      arrange(extracted_month) %>%
      collect() %>%
      transmute(num_flights = as.numeric(num_flights))
  }
  compare_tbls(list(tbl(impala, "flights"), nycflights13::flights), op = test_op)
})

test_that("month() with label = TRUE returns expected result", {
  check_impala()
  test_op <- function(x) {
    x %>%
      transmute(extracted_month = month(time_hour, label = TRUE)) %>%
      group_by(extracted_month) %>%
      summarise(num_flights = n()) %>%
      arrange(extracted_month) %>%
      collect() %>%
      transmute(num_flights = as.numeric(num_flights))
  }
  compare_tbls(list(tbl(impala, "flights"), nycflights13::flights), op = test_op)
})

test_that("isoweek() returns expected result", {
  check_impala()
  test_op <- function(x) {
    x %>%
      transmute(extracted_week = isoweek(time_hour)) %>%
      group_by(extracted_week) %>%
      summarise(num_flights = n()) %>%
      arrange(extracted_week) %>%
      collect() %>%
      transmute(num_flights = as.numeric(num_flights))
  }
  compare_tbls(list(tbl(impala, "flights"), nycflights13::flights), op = test_op)
})

test_that("yday() returns expected result", {
  skip("Skipping because of JDBC driver problem")
  check_impala()
  test_op <- function(x) {
    x %>%
      transmute(extracted_day = yday(time_hour)) %>%
      group_by(extracted_day) %>%
      summarise(num_flights = n()) %>%
      arrange(extracted_day) %>%
      collect() %>%
      transmute(num_flights = as.numeric(num_flights))
  }
  compare_tbls(list(tbl(impala, "flights"), nycflights13::flights), op = test_op)
})

test_that("day() returns expected result", {
  check_impala()
  test_op <- function(x) {
    x %>%
      transmute(extracted_day = day(time_hour)) %>%
      group_by(extracted_day) %>%
      summarise(num_flights = n()) %>%
      arrange(extracted_day) %>%
      collect() %>%
      transmute(num_flights = as.numeric(num_flights))
  }
  compare_tbls(list(tbl(impala, "flights"), nycflights13::flights), op = test_op)
})

test_that("mday() returns expected result", {
  check_impala()
  test_op <- function(x) {
    x %>%
      transmute(extracted_day = mday(time_hour)) %>%
      group_by(extracted_day) %>%
      summarise(num_flights = n()) %>%
      arrange(extracted_day) %>%
      collect() %>%
      transmute(num_flights = as.numeric(num_flights))
  }
  compare_tbls(list(tbl(impala, "flights"), nycflights13::flights), op = test_op)
})

test_that("wday() returns expected result", {
  skip("Skipping because of JDBC driver problem")
  check_impala()
  test_op <- function(x) {
    x %>%
      transmute(extracted_day = wday(time_hour)) %>%
      group_by(extracted_day) %>%
      summarise(num_flights = n()) %>%
      arrange(extracted_day) %>%
      collect() %>%
      transmute(num_flights = as.numeric(num_flights))
  }
  compare_tbls(list(tbl(impala, "flights"), nycflights13::flights), op = test_op)
})

test_that("wday() with label = TRUE returns expected result", {
  skip("Skipping because of JDBC driver problem")
  check_impala()
  test_op <- function(x) {
    x %>%
      transmute(extracted_day = wday(time_hour, label = TRUE)) %>%
      group_by(extracted_day) %>%
      summarise(num_flights = n()) %>%
      arrange(extracted_day) %>%
      collect() %>%
      transmute(num_flights = as.numeric(num_flights))
  }
  compare_tbls(list(tbl(impala, "flights"), nycflights13::flights), op = test_op)
})

test_that("wday() with label = TRUE and abbr = FALSE returns expected result", {
  skip("Skipping because of JDBC driver problem")
  check_impala()
  test_op <- function(x) {
    x %>%
      transmute(extracted_day = wday(time_hour, label = TRUE, abbr = FALSE)) %>%
      group_by(extracted_day) %>%
      summarise(num_flights = n()) %>%
      arrange(extracted_day) %>%
      collect() %>%
      transmute(num_flights = as.numeric(num_flights))
  }
  compare_tbls(list(tbl(impala, "flights"), nycflights13::flights), op = test_op)
})

test_that("hour() returns expected result", {
  check_impala()
  test_op <- function(x) {
    x %>%
      transmute(extracted_hour = hour(time_hour)) %>%
      group_by(extracted_hour) %>%
      summarise(num_flights = n()) %>%
      arrange(extracted_hour) %>%
      collect() %>%
      transmute(num_flights = as.numeric(num_flights))
  }
  compare_tbls(list(tbl(impala, "flights"), nycflights13::flights), op = test_op)
})

test_that("minute() returns expected result", {
  check_impala()
  test_op <- function(x) {
    x %>%
      transmute(date_time = as.POSIXct("2019-07-07 18:19:51")) %>%
      transmute(extracted_minute = minute(date_time)) %>%
      collect() %>%
      as.integer()
  }
  expect_true(
    compare(test_op(tbl(impala, "one_row")), 19L)$equal
  )
})

test_that("second() returns expected result", {
  check_impala()
  test_op <- function(x) {
    x %>%
      transmute(date_time = as.POSIXct("2019-07-07 18:19:51")) %>%
      transmute(extracted_second = second(date_time)) %>%
      collect() %>%
      as.integer()
  }
  expect_true(
    compare(test_op(tbl(impala, "one_row")), 51L)$equal
  )
})

test_that("floor_date() with unit = \"second\" returns expected result", {
  skip("Skip until test environment is upgraded to newer Impala version")
  check_impala()
  test_op <- function(x) {
    x %>%
      transmute(date_time = as.POSIXct("2019-07-07 18:19:51.42")) %>%
      transmute(truncated_date_time = floor_date(date_time, "second")) %>%
      collect() %>%
      as.character()
  }
  expect_true(
    compare(test_op(tbl(impala, "one_row")), "2019-07-07 18:19:51")$equal
  )
})

test_that("floor_date() with unit = \"hour\" returns expected result", {
  skip("Skip until test environment is upgraded to newer Impala version")
  check_impala()
  test_op <- function(x) {
    x %>%
      transmute(date_time = as.POSIXct("2019-07-07 18:19:51.42")) %>%
      transmute(truncated_date_time = floor_date(date_time, "hour")) %>%
      collect() %>%
      as.character()
  }
  expect_true(
    compare(test_op(tbl(impala, "one_row")), "2019-07-07 18:00:00")$equal
  )
})

test_that("floor_date() with unit = \"day\" returns expected result", {
  skip("Skip until test environment is upgraded to newer Impala version")
  check_impala()
  test_op <- function(x) {
    x %>%
      transmute(date_time = as.POSIXct("2019-07-07 18:19:51.42")) %>%
      transmute(truncated_date_time = floor_date(date_time, "day")) %>%
      collect() %>%
      as.character()
  }
  expect_true(
    compare(test_op(tbl(impala, "one_row")), "2019-07-07 00:00:00")$equal
  )
})
