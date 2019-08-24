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

test_that("log() returns expected result", {
  check_impala()
  test_op <- function(x) {
    x %>%
      rename_all(recode, Sepal.Length = "sepal_length") %>%
      transmute(log_sepal_length = log(sepal_length)) %>%
      arrange(log_sepal_length) %>%
      collect() %>%
      pull(1) %>%
      as.numeric() %>%
      round(4)
  }
  compare_tbls(list(tbl(impala, "iris"), datasets::iris), op = test_op)
})

test_that("log() with base specified returns expected result", {
  check_impala()
  test_op <- function(x) {
    x %>%
      rename_all(recode, Sepal.Length = "sepal_length") %>%
      transmute(log_sepal_length = log(sepal_length, 3)) %>%
      arrange(log_sepal_length) %>%
      collect() %>%
      pull(1) %>%
      as.numeric() %>%
      round(4)
  }
  compare_tbls(list(tbl(impala, "iris"), datasets::iris), op = test_op)
})

test_that("log10() returns expected result", {
  check_impala()
  test_op <- function(x) {
    x %>%
      rename_all(recode, Sepal.Length = "sepal_length") %>%
      transmute(log_sepal_length = log10(sepal_length)) %>%
      arrange(log_sepal_length) %>%
      collect() %>%
      pull(1) %>%
      as.numeric() %>%
      round(4)
  }
  compare_tbls(list(tbl(impala, "iris"), datasets::iris), op = test_op)
})

test_that("log2() returns expected result", {
  check_impala()
  test_op <- function(x) {
    x %>%
      rename_all(recode, Sepal.Length = "sepal_length") %>%
      transmute(log_sepal_length = log2(sepal_length)) %>%
      arrange(log_sepal_length) %>%
      collect() %>%
      pull(1) %>%
      as.numeric() %>%
      round(4)
  }
  compare_tbls(list(tbl(impala, "iris"), datasets::iris), op = test_op)
})

test_that("exp() returns expected result", {
  check_impala()
  test_op <- function(x) {
    x %>%
      rename_all(recode, Sepal.Length = "sepal_length") %>%
      transmute(exp_sepal_length = exp(sepal_length)) %>%
      arrange(exp_sepal_length) %>%
      collect() %>%
      pull(1) %>%
      as.numeric() %>%
      round(2)
  }
  compare_tbls(list(tbl(impala, "iris"), datasets::iris), op = test_op)
})

test_that("exp(1) returns expected result", {
  check_impala()
  expect_equal(
    tbl(impala, "one_row") %>% transmute(exp(1)) %>%
      collect() %>% pull(1) %>% as.numeric() %>% round(6),
    exp(1) %>% round(6)
  )
})

test_that("sqrt() returns expected result", {
  check_impala()
  test_op <- function(x) {
    x %>%
      rename_all(recode, Sepal.Length = "sepal_length") %>%
      transmute(sqrt_sepal_length = sqrt(sepal_length)) %>%
      arrange(sqrt_sepal_length) %>%
      collect() %>%
      pull(1) %>%
      as.numeric() %>%
      round(4)
  }
  compare_tbls(list(tbl(impala, "iris"), datasets::iris), op = test_op)
})

test_that("factorial() returns expected result on integers", {
  check_impala()
  test_op <- function(x) {
    x %>%
      rename_all(recode, Sepal.Length = "sepal_length") %>%
      transmute(fact = factorial(as.integer(sepal_length))) %>%
      collect() %>%
      pull(1) %>%
      as.integer()
  }
  compare_tbls(list(tbl(impala, "iris"), datasets::iris), op = test_op)
})

test_that("modulo operator (%%) returns expected result", {
  check_impala()
  test_op <- function(x) {
    x %>%
      rename_all(recode, Sepal.Length = "sepal_length") %>%
      transmute(sepal_length_mod_3 = sepal_length %% 3) %>%
      arrange(sepal_length_mod_3) %>%
      collect() %>%
      pull(1) %>%
      as.numeric() %>%
      round(4)
  }
  compare_tbls(list(tbl(impala, "iris"), datasets::iris), op = test_op)
})

test_that("integer division (%/%) returns expected result", {
  check_impala()
  expect_equal(
    tbl(impala, "one_row") %>% transmute(7L %/% 2L) %>%
      collect() %>% pull(1),
    3L
  )
})

test_that("pi returns expected result", {
  check_impala()
  expect_equal(
    tbl(impala, "one_row") %>% transmute(round(pi, 5)) %>%
      collect() %>% pull(1) %>% as.numeric() %>% round(5),
    3.14159
  )
})
