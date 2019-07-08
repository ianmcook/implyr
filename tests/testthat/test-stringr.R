context("stringr function translations")

library(stringr)

test_that("result of scalar str_c() is consistent with result on tbl_df", {
  check_impala()
  test_op <- function(x) {
    x %>%
      mutate(flight_code = str_c(carrier, as.character(flight))) %>%
      arrange(carrier, flight) %>% head(5) %>% collect() %>% select(flight_code)
  }
  compare_tbls(list(tbl(impala, "flights"), nycflights13::flights), op = test_op, convert = TRUE)
})


test_that("result of scalar str_c() including a literal is consistent with result on tbl_df", {
  check_impala()
  test_op <- function(x) {
    x %>%
      mutate(flight_code = str_c(carrier, "-", as.character(flight))) %>%
      arrange(carrier, flight) %>% head(5) %>% collect() %>% select(flight_code)
  }
  compare_tbls(list(tbl(impala, "flights"), nycflights13::flights), op = test_op, convert = TRUE)
})

test_that("error when using scalar str_c() with collapse", {
  check_impala()
  test_op <- function(x) {
    x %>%
      mutate(flight_code = str_c(carrier, as.character(flight), collapse = "")) %>%
      arrange(carrier, flight) %>% head(5) %>% collect() %>% select(flight_code)
  }
  expect_error(
    test_op(tbl(impala, "flights")),
    regexp = "not supported"
  )
})

test_that("str_c() with one arument and collapse works for aggregating", {
  check_impala()
  test_op <- function(x) {
    x %>%
      group_by(cyl) %>% summarise(gears = str_c(unique(as.character(gear)), collapse = " ")) %>%
      filter(cyl == 6) %>%
      select(gears) %>%
      collect() %>%
      pull(gears) -> y
    any(c("3 4 5", "3 5 4", "4 3 5", "4 5 3", "5 3 4", "5 4 3") %in% y)
  }
  expect_true(
    test_op(tbl(impala, "mtcars"))
  )
})

test_that("str_c() with multiple arguments and collapse works for aggregating", {
  check_impala()
  test_op <- function(x) {
    any(
      x %>%
        summarise(str_c(carrier, name, sep = ":", collapse = ";")) %>%
        collect() %>%
        pull(1) %>%
        grepl("9E:Endeavor Air Inc.;", ., fixed = TRUE),
      x %>%
        summarise(str_c(carrier, name, sep = ":", collapse = ";")) %>%
        collect() %>%
        pull(1) %>%
        grepl(";9E:Endeavor Air Inc.", ., fixed = TRUE)
    )
  }
  expect_true(
    test_op(tbl(impala, "airlines"))
  )
})

test_that("error when using str_c() without collapse for aggregating", {
  check_impala()
  test_op <- function(x) {
    x %>%
      group_by(cyl) %>% summarise(gears = str_c(unique(as.character(gear)))) %>%
      collect()
  }
  expect_error(
    test_op(tbl(impala, "mtcars")),
    regexp = "collapse"
  )
})

test_that("str_flatten() works for aggregating", {
  check_impala()
  test_op <- function(x) {
    x %>%
      group_by(cyl) %>% summarise(gears = str_flatten(unique(as.character(gear)), collapse = " ")) %>%
      filter(cyl == 6) %>%
      select(gears) %>%
      collect() %>%
      pull(gears) -> y
    any(c("3 4 5", "3 5 4", "4 3 5", "4 5 3", "5 3 4", "5 4 3") %in% y)
  }
  expect_true(
    test_op(tbl(impala, "mtcars"))
  )
})

test_that("str_trim() with side = \"both\" returns expected result", {
  check_impala()
  expect_equal(
    tbl(impala, "one_row") %>% transmute(trimmed = str_trim("  abc      ", side = "both")) %>% collect() %>% as.character(),
    "abc"
  )
})

test_that("str_trim() with side = \"left\" returns expected result", {
  check_impala()
  expect_equal(
    tbl(impala, "one_row") %>% transmute(trimmed = str_trim("  abc      ", side = "left")) %>% collect() %>% as.character(),
    "abc      "
  )
})

test_that("str_trim() with side = \"right\" returns expected result", {
  check_impala()
  expect_equal(
    tbl(impala, "one_row") %>% transmute(trimmed = str_trim("  abc      ", side = "right")) %>% collect() %>% as.character(),
    "  abc"
  )
})

test_that("str_to_lower() returns expected result", {
  check_impala()
  expect_equal(
    tbl(impala, "one_row") %>% transmute(lower = str_to_lower("ABC")) %>% collect() %>% as.character(),
    "abc"
  )
})

test_that("str_to_upper() returns expected result", {
  check_impala()
  expect_equal(
    tbl(impala, "one_row") %>% transmute(upper = str_to_upper("abc")) %>% collect() %>% as.character(),
    "ABC"
  )
})

test_that("str_to_title() returns expected result", {
  check_impala()
  expect_equal(
    tbl(impala, "one_row") %>% transmute(upper = str_to_title("hElLo wOrLd!")) %>% collect() %>% as.character(),
    "Hello World!"
  )
})

test_that("str_length() returns expected result", {
  check_impala()
  expect_equal(
    tbl(impala, "one_row") %>% transmute(len = str_length("abc")) %>% collect() %>% as.integer(),
    3L
  )
})

test_that("str_sub() with positive values for all arguments returns expected result", {
  check_impala()
  test_op <- function(x) {
    x %>%
      transmute(result = str_sub(input, 2, 4)) %>%
      as.data.frame(stringsAsFactors = FALSE) %>%
      pull(1) %>%
      as.character()
  }
  compare_tbls(
    list(
      tbl(impala, "one_row") %>% transmute(input = "abcdefg"),
      tibble(input = "abcdefg")
    ),
    op = test_op, convert = TRUE
  )
})

test_that("str_sub() with no end argument returns expected result", {
  check_impala()
  test_op <- function(x) {
    x %>%
      transmute(result = str_sub(input, 3)) %>%
      as.data.frame(stringsAsFactors = FALSE) %>%
      pull(1) %>%
      as.character()
  }
  compare_tbls(
    list(
      tbl(impala, "one_row") %>% transmute(input = "abcdefg"),
      tibble(input = "abcdefg")
    ),
    op = test_op, convert = TRUE
  )
})

test_that("str_sub() with negative start argument and no end argument returns expected result", {
  check_impala()
  test_op <- function(x) {
    x %>%
      transmute(result = str_sub(input, -4)) %>%
      as.data.frame(stringsAsFactors = FALSE) %>%
      pull(1) %>%
      as.character()
  }
  compare_tbls(
    list(
      tbl(impala, "one_row") %>% transmute(input = "abcdefg"),
      tibble(input = "abcdefg")
    ),
    op = test_op, convert = TRUE
  )
})

test_that("str_sub() with negative start argument returns expected result", {
  check_impala()
  test_op <- function(x) {
    x %>%
      transmute(result = str_sub(input, -6, 4)) %>%
      as.data.frame(stringsAsFactors = FALSE) %>%
      pull(1) %>%
      as.character()
  }
  compare_tbls(
    list(
      tbl(impala, "one_row") %>% transmute(input = "abcdefg"),
      tibble(input = "abcdefg")
    ),
    op = test_op, convert = TRUE
  )
})

test_that("str_sub() with negative end argument returns expected result", {
  check_impala()
  test_op <- function(x) {
    x %>%
      transmute(result = str_sub(input, 2, -3)) %>%
      as.data.frame(stringsAsFactors = FALSE) %>%
      pull(1) %>%
      as.character()
  }
  compare_tbls(
    list(
      tbl(impala, "one_row") %>% transmute(input = "abcdefg"),
      tibble(input = "abcdefg")
    ),
    op = test_op, convert = TRUE
  )
})

test_that("str_sub() with zero end argument returns expected result", {
  check_impala()
  test_op <- function(x) {
    x %>%
      transmute(result = str_sub(input, 2, 0)) %>%
      as.data.frame(stringsAsFactors = FALSE) %>%
      pull(1) %>%
      as.character()
  }
  compare_tbls(
    list(
      tbl(impala, "one_row") %>% transmute(input = "abcdefg"),
      tibble(input = "abcdefg")
    ),
    op = test_op, convert = TRUE
  )
})

test_that("str_sub() with negative start and end arguments returns expected result", {
  check_impala()
  test_op <- function(x) {
    x %>%
      transmute(result = str_sub(input, -5, -2)) %>%
      as.data.frame(stringsAsFactors = FALSE) %>%
      pull(1) %>%
      as.character()
  }
  compare_tbls(
    list(
      tbl(impala, "one_row") %>% transmute(input = "abcdefg"),
      tibble(input = "abcdefg")
    ),
    op = test_op, convert = TRUE
  )
})
