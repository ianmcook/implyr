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
  skip_for_codecov()
  check_impala()
  test_op <- function(x) {
    set.seed(seed = NULL)
    table_name <-
      paste0(sample(letters, 10, replace = TRUE), collapse = "")
    dbExecute(impala, paste0("DROP TABLE IF EXISTS ", table_name))
    Sys.sleep(2)
    x %>% head(10) %>% arrange(day) %>% compute(table_name, temporary = FALSE)
    # clean up
    Sys.sleep(2)
    dbExecute(impala, paste0("DROP TABLE IF EXISTS ", table_name))
    invisible(NULL)
  }
  expect_warning(test_op(tbl(impala, "flights")))
})

test_that("no warning when no arrange() before compute()", {
  skip_for_codecov()
  check_impala()
  test_op <- function(x) {
    set.seed(seed = NULL)
    table_name <-
      paste0(sample(letters, 10, replace = TRUE), collapse = "")
    dbExecute(impala, paste0("DROP TABLE IF EXISTS ", table_name))
    Sys.sleep(2)
    x %>% head(10) %>% compute(table_name, temporary = FALSE)
    # clean up
    Sys.sleep(2)
    dbExecute(impala, paste0("DROP TABLE IF EXISTS ", table_name))
    invisible(NULL)
  }
  expect_warning(test_op(tbl(impala, "flights")), regexp = NA)
})

test_that("deprecation warning when using str_collapse()", {
  check_impala()
  test_op <- function(x) {
    x %>%
      group_by(cyl) %>%
        summarise(
          gears = str_collapse(unique(as.character(gear)), collapse = " ")
        ) %>%
      collect()
  }
  expect_warning(test_op(tbl(impala, "mtcars")), regexp = "deprecated")
})
