context("warnings")

test_that("warning when arrange() before other verbs", {
  check_impala()
  test_op <- function(x) {
    # sql_build(x %>% arrange(year) %>% head(10) %>% arrange(day))
    x %>% arrange(year) %>% head(10) %>% arrange(day) %>% collect()
  }
  expect_warning(test_op(tbl(impala, "flights")))
})
