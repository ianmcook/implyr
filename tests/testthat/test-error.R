context("expected errors")

test_that("error when using unsupported window function median()", {
  check_impala()
  test_op <- function(x) {
    x %>% group_by(playerid) %>% mutate(median(g)) %>% collect()
  }
  expect_error(
    test_op(tbl(impala, in_schema("lahman", "batting")))
  )
})

test_that("error when using unsupported window function n_distinct()", {
  check_impala()
  test_op <- function(x) {
    x %>% group_by(playerid) %>% mutate(n_distinct(g)) %>% collect()
  }
  expect_error(
    test_op(tbl(impala, in_schema("lahman", "batting")))
  )
})

test_that("error when using unsupported window function ndv()", {
  check_impala()
  test_op <- function(x) {
    x %>% group_by(playerid) %>% mutate(ndv(g)) %>% collect()
  }
  expect_error(
    test_op(tbl(impala, in_schema("lahman", "batting"))),
    regexp = "not.supported"
  )
})

test_that("error when using unsupported window function sd()", {
  check_impala()
  test_op <- function(x) {
    x %>% group_by(playerid) %>% mutate(sd(g)) %>% collect()
  }
  expect_error(
    test_op(tbl(impala, in_schema("lahman", "batting"))),
    regexp = "not.supported"
  )
})

test_that("error when using unsupported window function var()", {
  check_impala()
  test_op <- function(x) {
    x %>% group_by(playerid) %>% mutate(var(g)) %>% collect()
  }
  expect_error(
    test_op(tbl(impala, in_schema("lahman", "batting"))),
    regexp = "not.supported"
  )
})
