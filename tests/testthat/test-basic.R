context("basic tests")

test_that("tbl_impala matches tbl_df", {
  check_impala()
  test_op <- function(x) {
    x %>% arrange(carrier) %>% collect()
  }
  compare_tbls(list(tbl(impala, "airlines"), nycflights13::airlines), op = test_op)
})

test_that("expect failure", {
  test_op <- function(x) x
  compare_tbls(list(iris, mtcars), op = test_op)
})
