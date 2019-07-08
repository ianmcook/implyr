context("Impala date and time functions")

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
      distinct(extracted_minute) %>%
      collect() %>%
      as.integer()
  }
  expect_true(
    compare(test_op(tbl(impala, "iris")), 19L)$equal
  )
})

test_that("second() returns expected result", {
  check_impala()
  test_op <- function(x) {
    x %>%
      transmute(date_time = as.POSIXct("2019-07-07 18:19:51")) %>%
      transmute(extracted_second = second(date_time)) %>%
      distinct(extracted_second) %>%
      collect() %>%
      as.integer()
  }
  expect_true(
    compare(test_op(tbl(impala, "iris")), 51L)$equal
  )
})
