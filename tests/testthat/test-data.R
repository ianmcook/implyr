context("check test data")

test_that("src_tbls() lists flights and airlines tables", {
  check_impala()
  expect_true(all(c("airlines", "flights") %in% src_tbls(impala)))
})

test_that("flights tbl_impala has same column names as flights tbl_df", {
  check_impala()
  test_op <- function(x) {
    x %>% colnames()
  }
  expect_true(
    compare(test_op(tbl(impala, "flights")), test_op(nycflights13::flights))$equal
  )
})

test_that("flights tbl_impala has same number of rows as flights tbl_df", {
  check_impala()
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

test_that("src_databases() lists _impala_builtins, default, and lahman databases", {
  check_impala()
  expect_true(all(c("_impala_builtins", "default", "lahman") %in% src_databases(impala)))
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
  test_op <- function(x) {
    x %>% summarize(n()) %>% collect() %>% as.integer()
  }
  expect_true(
    compare(test_op(tbl(impala, in_schema("lahman", "batting"))), test_op(Lahman::Batting))$equal
  )
})

test_that("src_tbls() lists one_row table", {
  check_impala()
  expect_true("one_row" %in% src_tbls(impala))
})

test_that("one_row table has exactly one row", {
  check_impala()
  expect_equal(
    tbl(impala, "one_row") %>% tally() %>% pull(1) %>% as.integer(),
    1L
  )
})

test_that("src_tbls() lists unicode_test table", {
  check_impala()
  expect_true("unicode_test" %in% src_tbls(impala))
})
