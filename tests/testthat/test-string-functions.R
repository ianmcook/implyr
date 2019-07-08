context("String function translations")

test_that("result of scalar paste0() is consistent with result on tbl_df", {
  check_impala()
  test_op <- function(x) {
    x %>%
      mutate(flight_code = paste0(carrier, as.character(flight))) %>%
      arrange(carrier, flight) %>% head(5) %>% collect() %>% select(flight_code)
  }
  compare_tbls(list(tbl(impala, "flights"), nycflights13::flights), op = test_op, convert = TRUE)
})

test_that("result of scalar paste0() including a literal is consistent with result on tbl_df", {
  check_impala()
  test_op <- function(x) {
    x %>%
      mutate(flight_code = paste0(carrier, "-", as.character(flight))) %>%
      arrange(carrier, flight) %>% head(5) %>% collect() %>% select(flight_code)
  }
  compare_tbls(list(tbl(impala, "flights"), nycflights13::flights), op = test_op, convert = TRUE)
})

test_that("error when using scalar paste0() with collapse", {
  check_impala()
  test_op <- function(x) {
    x %>%
      mutate(flight_code = paste0(carrier, as.character(flight), collapse = "")) %>%
      arrange(carrier, flight) %>% head(5) %>% collect() %>% select(flight_code)
  }
  expect_error(
    test_op(tbl(impala, "flights")),
    regexp = "not supported"
  )
})

test_that("result of scalar paste() is consistent with result on tbl_df", {
  check_impala()
  test_op <- function(x) {
    x %>%
      mutate(flight_code = paste(carrier, as.character(flight))) %>%
      arrange(carrier, flight) %>% head(5) %>% collect() %>% select(flight_code)
  }
  compare_tbls(list(tbl(impala, "flights"), nycflights13::flights), op = test_op, convert = TRUE)
})


test_that("result of scalar paste() including a literal is consistent with result on tbl_df", {
  check_impala()
  test_op <- function(x) {
    x %>%
      mutate(flight_code = paste(carrier, "-", as.character(flight))) %>%
      arrange(carrier, flight) %>% head(5) %>% collect() %>% select(flight_code)
  }
  compare_tbls(list(tbl(impala, "flights"), nycflights13::flights), op = test_op, convert = TRUE)
})

test_that("error when using scalar paste() with collapse", {
  check_impala()
  test_op <- function(x) {
    x %>%
      mutate(flight_code = paste(carrier, as.character(flight), collapse = "")) %>%
      arrange(carrier, flight) %>% head(5) %>% collect() %>% select(flight_code)
  }
  expect_error(
    test_op(tbl(impala, "flights")),
    regexp = "not supported"
  )
})

test_that("paste0() with one argument and collapse works for aggregating", {
  check_impala()
  test_op <- function(x) {
    x %>%
      group_by(cyl) %>% summarise(gears = paste0(unique(as.character(gear)), collapse = " ")) %>%
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

test_that("paste0() with multiple arguments and collapse works for aggregating", {
  check_impala()
  test_op <- function(x) {
    any(
      x %>%
        summarise(paste0(carrier, name, collapse = ";")) %>%
        collect() %>%
        pull(1) %>%
        grepl("9EEndeavor Air Inc.;", ., fixed = TRUE),
      x %>%
        summarise(paste0(carrier, name, collapse = ";")) %>%
        collect() %>%
        pull(1) %>%
        grepl(";9EEndeavor Air Inc.", ., fixed = TRUE)
    )
  }
  expect_true(
    test_op(tbl(impala, "airlines"))
  )
})

test_that("error when using paste0() without collapse for aggregating", {
  check_impala()
  test_op <- function(x) {
    x %>%
      group_by(cyl) %>% summarise(gears = paste0(unique(as.character(gear)))) %>%
      collect()
  }
  expect_error(
    test_op(tbl(impala, "mtcars")),
    regexp = "collapse"
  )
})

test_that("paste() with one arument and collapse works for aggregating", {
  check_impala()
  test_op <- function(x) {
    x %>%
      group_by(cyl) %>% summarise(gears = paste(unique(as.character(gear)), collapse = " ")) %>%
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

test_that("paste() with multiple arguments and collapse works for aggregating", {
  check_impala()
  test_op <- function(x) {
    any(
      x %>%
        summarise(paste(carrier, name, sep = ":", collapse = ";")) %>%
        collect() %>%
        pull(1) %>%
        grepl("9E:Endeavor Air Inc.;", ., fixed = TRUE),
      x %>%
        summarise(paste(carrier, name, sep = ":", collapse = ";")) %>%
        collect() %>%
        pull(1) %>%
        grepl(";9E:Endeavor Air Inc.", ., fixed = TRUE)
    )
  }
  expect_true(
    test_op(tbl(impala, "airlines"))
  )
})

test_that("error when using paste() without collapse for aggregating", {
  check_impala()
  test_op <- function(x) {
    x %>%
      group_by(cyl) %>% summarise(gears = paste(unique(as.character(gear)))) %>%
      collect()
  }
  expect_error(
    test_op(tbl(impala, "mtcars")),
    regexp = "collapse"
  )
})

test_that("trim() returns expected result", {
  check_impala()
  expect_equal(
    tbl(impala, "one_row") %>% transmute(trimmed = trim("  abc      ")) %>% collect() %>% as.character(),
    "abc"
  )
})

test_that("trimws() with side = \"both\" returns expected result", {
  check_impala()
  expect_equal(
    tbl(impala, "one_row") %>% transmute(trimmed = trimws("  abc      ", side = "both")) %>% collect() %>% as.character(),
    "abc"
  )
})

test_that("trimws() with side = \"left\" returns expected result", {
  check_impala()
  expect_equal(
    tbl(impala, "one_row") %>% transmute(trimmed = trimws("  abc      ", side = "left")) %>% collect() %>% as.character(),
    "abc      "
  )
})

test_that("trimws() with side = \"right\" returns expected result", {
  check_impala()
  expect_equal(
    tbl(impala, "one_row") %>% transmute(trimmed = trimws("  abc      ", side = "right")) %>% collect() %>% as.character(),
    "  abc"
  )
})

test_that("tolower() returns expected result", {
  check_impala()
  expect_equal(
    tbl(impala, "one_row") %>% transmute(lower = tolower("ABC")) %>% collect() %>% as.character(),
    "abc"
  )
})

test_that("toupper() returns expected result", {
  check_impala()
  expect_equal(
    tbl(impala, "one_row") %>% transmute(upper = toupper("abc")) %>% collect() %>% as.character(),
    "ABC"
  )
})

test_that("nchar() returns expected result", {
  check_impala()
  expect_equal(
    tbl(impala, "one_row") %>% transmute(len = nchar("abc")) %>% collect() %>% as.integer(),
    3L
  )
})

test_that("substr() returns expected result", {
  check_impala()
  expect_equal(
    tbl(impala, "one_row") %>%
      transmute(sub = substr("Never odd or even", 7, 9)) %>%
      collect() %>%
      as.character(),
    "odd"
  )
})
