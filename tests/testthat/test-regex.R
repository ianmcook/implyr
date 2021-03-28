context("Regular expression translations")

test_that("grepl() returns expected result", {
  check_impala()
  test_op <- function(x) {
    x %>%
      filter(grepl("^v", species)) %>%
      tally() %>%
      collect() %>%
      as.integer()
  }
  expect_true(
    compare(test_op(tbl(impala, "iris")), 100L)
  )
})

test_that("grepl() ignores case by default and returns expected result", {
  check_impala()
  test_op <- function(x) {
    x %>%
      filter(grepl("^V", species)) %>%
      tally() %>%
      collect() %>%
      pull(1) %>%
      as.integer()
  }
  expect_true(
    compare(test_op(tbl(impala, "iris")), 0L)
  )
})

test_that("grepl() returns expected result with ignore.case = TRUE", {
  check_impala()
  test_op <- function(x) {
    x %>%
      filter(grepl("^V", species, ignore.case = TRUE)) %>%
      tally() %>%
      collect() %>%
      pull(1) %>%
      as.integer()
  }
  expect_true(
    compare(test_op(tbl(impala, "iris")), 100L)
  )
})

test_that("gsub() returns expected result", {
  check_impala()
  test_op <- function(x) {
    x %>%
      transmute(mangled = gsub("v", "b", species)) %>%
      distinct(mangled) %>%
      arrange(desc(mangled)) %>%
      collect() %>%
      pull(1)
  }
  expect_true(
    compare(test_op(tbl(impala, "iris")), c("setosa", "birginica", "bersicolor"))
  )
})
