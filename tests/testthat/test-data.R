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
  test_op <- function(x) {
    x %>% summarize(n()) %>% collect() %>% as.integer()
  }
  compare(test_op(tbl(impala, "flights")), test_op(nycflights13::flights))
})

test_that("airlines tbl_impala matches airlines tbl_df", {
  check_impala()
  test_op <- function(x) {
    x %>% arrange(carrier) %>% collect()
  }
  compare_tbls(list(tbl(impala, "airlines"), nycflights13::airlines), op = test_op)
})
