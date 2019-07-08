context("Bitwise function translations")

test_that("bitwNot() returns expected result", {
  check_impala()
  expect_equal(
    tbl(impala, "one_row") %>% transmute(result = bitwNot(127L)) %>% collect() %>% pull(1) %>% as.integer(),
    -128L
  )
})

test_that("bitwAnd() returns expected result", {
  check_impala()
  expect_equal(
    tbl(impala, "one_row") %>% transmute(result = bitwAnd(255L, 32767L)) %>% collect() %>% pull(1) %>% as.integer(),
    255L
  )
})

test_that("bitwOr() returns expected result", {
  check_impala()
  expect_equal(
    tbl(impala, "one_row") %>% transmute(result = bitwOr(16L, 48L)) %>% collect() %>% pull(1) %>% as.integer(),
    48L
  )
})

test_that("bitwXor() returns expected result", {
  check_impala()
  expect_equal(
    tbl(impala, "one_row") %>% transmute(result = bitwXor(8L, 4L)) %>% collect() %>% pull(1) %>% as.integer(),
    12L
  )
})

test_that("bitwShiftL() returns expected result", {
  check_impala()
  expect_equal(
    tbl(impala, "one_row") %>% transmute(result = bitwShiftL(127L, 5L)) %>% collect() %>% pull(1) %>% as.integer(),
    -32L
  )
})

test_that("bitwShiftR() returns expected result", {
  check_impala()
  expect_equal(
    tbl(impala, "one_row") %>% transmute(result = bitwShiftR(-1L, 5L)) %>% collect() %>% pull(1) %>% as.integer(),
    7L
  )
})
