context("expected warnings")

test_that("warning when arrange() before other verbs", {
  check_impala()
  test_op <- function(x) {
    x %>% arrange(year) %>% head(10) %>% arrange(day) %>% collect()
  }
  expect_warning(test_op(tbl(impala, "flights")))
})

test_that("no warning when no arrange() before other verbs", {
  check_impala()
  test_op <- function(x) {
    x %>% head(10) %>% arrange(day) %>% collect()
  }
  expect_warning(test_op(tbl(impala, "flights")), regexp = NA)
})

test_that("warning when arrange() before compute()", {
  check_impala()
  test_op <- function(x) {
    set.seed(seed = NULL)
    table_name <- paste0(sample(letters, 10, replace = TRUE), collapse = "")
    x %>% head(10) %>% arrange(day) %>% compute(table_name, temporary = FALSE)
    # clean up
    dbExecute(impala, paste0("DROP TABLE ", table_name))
  }
  expect_warning(test_op(tbl(impala, "flights")))
})

test_that("no warning when no arrange() before compute()", {
  check_impala()
  test_op <- function(x) {
    set.seed(seed = NULL)
    table_name <- paste0(sample(letters, 10, replace = TRUE), collapse = "")
    x %>% head(10) %>% compute(table_name, temporary = FALSE)
    # clean up
    dbExecute(impala, paste0("DROP TABLE ", table_name))
  }
  expect_warning(test_op(tbl(impala, "flights")), regexp = NA)
})
