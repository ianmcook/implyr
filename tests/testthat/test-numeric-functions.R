context("Impala numeric functions")

test_that("round() returns result with expected dimension", {
  check_impala()
  test_op <- function(x) {
    x %>%
      rename_all(recode, Sepal.Length = "sepal_length") %>%
      transmute(sepal_length_whole_number = round(sepal_length)) %>%
      collect() %>%
      dim()
  }
  compare_tbls(list(tbl(impala, "iris"), datasets::iris), op = test_op)
})

test_that("floor() returns expected result", {
  check_impala()
  test_op <- function(x) {
    x %>%
      rename_all(recode, Sepal.Length = "sepal_length") %>%
      transmute(sepal_length_whole_number = floor(sepal_length)) %>%
      arrange(sepal_length_whole_number) %>%
      collect() %>%
      pull(1) %>%
      as.numeric()
  }
  compare_tbls(list(tbl(impala, "iris"), datasets::iris), op = test_op)
})

test_that("ceiling() returns expected result", {
  check_impala()
  test_op <- function(x) {
    x %>%
      rename_all(recode, Sepal.Length = "sepal_length") %>%
      transmute(sepal_length_whole_number = ceiling(sepal_length)) %>%
      arrange(sepal_length_whole_number) %>%
      collect() %>%
      pull(1) %>%
      as.numeric()
  }
  compare_tbls(list(tbl(impala, "iris"), datasets::iris), op = test_op)
})

