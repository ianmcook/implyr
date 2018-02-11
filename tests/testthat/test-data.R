context("check test data")

test_that("flights and airlines tables exist", {
  check_impala()
  expect_true(all(c("airlines", "flights") %in% src_tbls(impala)))
})

test_that("flights tbl_impala has same column names as flights tbl_df", {
  check_impala()
  test_op <- function(x) {
    x %>% colnames()
  }
  compare(test_op(tbl(impala, "flights")), test_op(nycflights13::flights))
})

test_that("flights tbl_impala has same number of rows as flights tbl_df", {
  check_impala()
  skip_on_travis() # Test fails for unknown reason on Travis CI but succeeds in other test environments
  test_op <- function(x) {
    x %>% summarize(n()) %>% collect() %>% as.integer()
  }
  expect_true(
    compare(test_op(tbl(impala, "flights")), test_op(nycflights13::flights))$equal
  )
})

test_that("airlines tbl_impala matches airlines tbl_df", {
  check_impala()
  test_op <- function(x) {
    x %>% arrange(carrier) %>% collect()
  }
  compare_tbls(list(tbl(impala, "airlines"), nycflights13::airlines), op = test_op)
})

test_that("batting table exists in lahman database", {
  check_impala()
  expect_true("batting" %in% dbGetQuery(impala, "SHOW TABLES IN lahman")$name)
})

test_that("batting tbl_impala has same column names as Batting tbl_df (ignoring case)", {
  check_impala()
  test_op <- function(x) {
    x %>% colnames() %>% tolower()
  }
  expect_true(
    compare(test_op(tbl(impala, in_schema("lahman", "batting"))), test_op(Lahman::Batting))$equal
  )
})

test_that("batting tbl_impala has same number of rows as Batting tbl_df", {
  check_impala()
  skip_on_travis() # Test fails for unknown reason on Travis CI but succeeds in other test environments
  test_op <- function(x) {
    x %>% summarize(n()) %>% collect() %>% as.integer()
  }
  expect_true(
    compare(test_op(tbl(impala, in_schema("lahman", "batting"))), test_op(Lahman::Batting))$equal
  )
})
