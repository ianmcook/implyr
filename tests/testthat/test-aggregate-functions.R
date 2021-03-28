context("Impala aggregate functions")

test_that("median() returns result with expected dimension", {
  check_impala()
  test_op <- function(x) {
    x %>%
      group_by(origin) %>%
      summarise(median_dep_delay = median(dep_delay, na.rm = TRUE)) %>%
      collect() %>%
      dim()
  }
  expect_true(
    compare(test_op(tbl(impala, "flights")), c(3, 2))
  )
})

test_that("mean() returns expected result", {
  check_impala()
  test_op <- function(x) {
    x %>%
      group_by(origin) %>%
      summarise(mean_dep_delay = mean(dep_delay, na.rm = TRUE)) %>%
      collect() %>%
      dim()
  }
  compare_tbls(list(tbl(impala, "flights"), nycflights13::flights), op = test_op)
})

test_that("n() returns expected result", {
  check_impala()
  test_op <- function(x) {
    x %>%
      summarise(num_flights = n()) %>%
      collect() %>%
      dim()
  }
  compare_tbls(list(tbl(impala, "flights"), nycflights13::flights), op = test_op)
})


test_that("min() returns expected result", {
  check_impala()
  test_op <- function(x) {
    x %>%
      group_by(origin) %>%
      summarise(min_dep_delay = min(dep_delay, na.rm = TRUE)) %>%
      collect() %>%
      dim()
  }
  compare_tbls(list(tbl(impala, "flights"), nycflights13::flights), op = test_op)
})

test_that("max() returns expected result", {
  check_impala()
  test_op <- function(x) {
    x %>%
      group_by(origin) %>%
      summarise(max_dep_delay = max(dep_delay, na.rm = TRUE)) %>%
      collect() %>%
      dim()
  }
  compare_tbls(list(tbl(impala, "flights"), nycflights13::flights), op = test_op)
})

test_that("ndv() returns result with expected dimension", {
  check_impala()
  test_op <- function(x) {
    x %>%
      group_by(origin) %>%
      summarise(approx_num_dest = ndv(dest, na.rm = TRUE)) %>%
      collect() %>%
      dim()
  }
  expect_true(
    compare(test_op(tbl(impala, "flights")), c(3, 2))
  )
})

test_that("sd() returns expected result", {
  check_impala()
  test_op <- function(x) {
    x %>%
      group_by(origin) %>%
      summarise(sd_dep_delay = sd(dep_delay, na.rm = TRUE)) %>%
      collect() %>%
      dim()
  }
  compare_tbls(list(tbl(impala, "flights"), nycflights13::flights), op = test_op)
})

test_that("sum() returns expected result", {
  check_impala()
  test_op <- function(x) {
    x %>%
      group_by(origin) %>%
      summarise(sum_dep_delay = sum(dep_delay, na.rm = TRUE)) %>%
      collect() %>%
      dim()
  }
  compare_tbls(list(tbl(impala, "flights"), nycflights13::flights), op = test_op)
})


test_that("var() returns expected result", {
  check_impala()
  test_op <- function(x) {
    x %>%
      group_by(origin) %>%
      summarise(var_dep_delay = var(dep_delay, na.rm = TRUE)) %>%
      collect() %>%
      dim()
  }
  compare_tbls(list(tbl(impala, "flights"), nycflights13::flights), op = test_op)
})
