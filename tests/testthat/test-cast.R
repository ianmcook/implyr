context("Type conversion")

test_that("as.character() returns column of type string", {
  check_impala()
  expect_equal(
    column_type(tbl(impala, "flights") %>% transmute(year = as.character(year))),
    "string"
  )
})

test_that("as.character() returns column of type string when no result column name is specified", {
  check_impala()
  expect_equal(
    column_type(tbl(impala, "flights") %>% transmute(as.character(year))),
    "string"
  )
})

test_that("as.string() returns column of type string", {
  check_impala()
  expect_equal(
    column_type(tbl(impala, "flights") %>% transmute(year = as.string(year))),
    "string"
  )
})

test_that("as.char() returns column of type char with specified length", {
  skip("Skipping because some versions of JDBC driver transform to string")
  check_impala()
  expect_equal(
    column_type(tbl(impala, "flights") %>% transmute(year = as.char(year, 4))),
    "char(4)"
  )
})

# do not test as.varchar(); the returned length will not necessarily match the specified length

test_that("as.boolean() returns column of type boolean", {
  check_impala()
  expect_equal(
    column_type(tbl(impala, "flights") %>% transmute(even_number_month = as.boolean(month %% 2))),
    "boolean"
  )
})

test_that("as.logical() returns column of type boolean", {
  check_impala()
  expect_equal(
    column_type(tbl(impala, "flights") %>% transmute(even_number_month = as.logical(month %% 2))),
    "boolean"
  )
})

test_that("as.numeric() returns column of type double", {
  check_impala()
  expect_equal(
    column_type(
      tbl(impala, "flights") %>%
        transmute(tailnum_digits = as.numeric(regexp_replace(tailnum, '[^[:digit:]]', '')))
    ),
    "double"
  )
})

test_that("as.int() returns column of type int", {
  check_impala()
  expect_equal(
    column_type(tbl(impala, "flights") %>% transmute(month_rounded = as.int(month + (day / 31)))),
    "int"
  )
})

test_that("as.int() returns column of type int", {
  check_impala()
  expect_equal(
    column_type(tbl(impala, "flights") %>% transmute(month_rounded = as.int(month + (day / 31)))),
    "int"
  )
})

test_that("as.integer() returns column of type int", {
  check_impala()
  expect_equal(
    column_type(tbl(impala, "flights") %>% transmute(month_rounded = as.integer(month + (day / 31)))),
    "int"
  )
})

test_that("as.bigint() returns column of type bigint", {
  check_impala()
  expect_equal(
    column_type(tbl(impala, "flights") %>% transmute(big_year = as.bigint(year))),
    "bigint"
  )
})

test_that("as.smallint() returns column of type smallint", {
  check_impala()
  expect_equal(
    column_type(tbl(impala, "flights") %>% transmute(small_month = as.smallint(month))),
    "smallint"
  )
})

test_that("as.tinyint() returns column of type tinyint", {
  check_impala()
  expect_equal(
    column_type(tbl(impala, "flights") %>% transmute(tiny_century = as.tinyint(year / 100))),
    "tinyint"
  )
})

test_that("as.double() returns column of type double", {
  check_impala()
  expect_equal(
    column_type(
      tbl(impala, "flights") %>% transmute(double_double_year = as.double(2 * year))
    ),
    "double"
  )
})

# do not test as.real(); might return float or double

test_that("as.float() returns column of type float", {
  skip("Skipping because some versions of JDBC driver transform to double")
  check_impala()
  expect_equal(
    column_type(
      tbl(impala, "flights") %>% transmute(float_year = as.float(year + 1))
    ),
    "float"
  )
})

test_that("as.single() returns column of type float", {
  skip("Skipping because some versions of JDBC driver transform to double")
  check_impala()
  expect_equal(
    column_type(
      tbl(impala, "flights") %>% transmute(single_year = as.single(year + 1))
    ),
    "float"
  )
})

test_that("as.decimal() returns column of type decimal with specified precision and scale", {
  check_impala()
  expect_equal(
    column_type(
      tbl(impala, "flights") %>% transmute(year_frac_month = as.decimal(year + (month / 12), 6, 2))
    ),
    "decimal(6,2)"
  )
})

test_that("as.timestamp() returns column of type timestamp", {
  check_impala()
  expect_equal(
    column_type(
      tbl(impala, "flights") %>% transmute(
        ymd_hms = as.timestamp(paste0(
          as.character(year), "-", lpad(as.character(month), 2L, "0"), "-", lpad(as.character(day), 2L, "0"), " ",
          lpad(as.character(as.integer(dep_time / 100)), 2L, "0"), ":", lpad(as.character(dep_time %% 100), 2L, "0"), ":00"
        )
      ))
    ),
    "timestamp"
  )
})

test_that("as.datetime() returns column of type timestamp", {
  check_impala()
  expect_equal(
    column_type(
      tbl(impala, "flights") %>% transmute(
        ymd_hms = as.datetime(paste0(
          as.character(year), "-", lpad(as.character(month), 2L, "0"), "-", lpad(as.character(day), 2L, "0"), " ",
          lpad(as.character(as.integer(dep_time / 100)), 2L, "0"), ":", lpad(as.character(dep_time %% 100), 2L, "0"), ":00"
        )
        ))
    ),
    "timestamp"
  )
})

test_that("as_datetime() returns column of type timestamp", {
  check_impala()
  expect_equal(
    column_type(
      tbl(impala, "flights") %>% transmute(
        ymd_hms = as_datetime(paste0(
          as.character(year), "-", lpad(as.character(month), 2L, "0"), "-", lpad(as.character(day), 2L, "0"), " ",
          lpad(as.character(as.integer(dep_time / 100)), 2L, "0"), ":", lpad(as.character(dep_time %% 100), 2L, "0"), ":00"
        )
        ))
    ),
    "timestamp"
  )
})

test_that("as.POSIXct() returns column of type timestamp", {
  check_impala()
  expect_equal(
    column_type(
      tbl(impala, "flights") %>% transmute(
        ymd_hms = as.POSIXct(paste0(
          as.character(year), "-", lpad(as.character(month), 2L, "0"), "-", lpad(as.character(day), 2L, "0"), " ",
          lpad(as.character(as.integer(dep_time / 100)), 2L, "0"), ":", lpad(as.character(dep_time %% 100), 2L, "0"), ":00"
        )
        ))
    ),
    "timestamp"
  )
})

test_that("as.Date() returns date part of timestamp", {
  check_impala()
  # do not explicitly check type of returned column like in timestamp tests
  # because:
  # in older versions of Impala, to_date() returns column of type STRING
  # in newer versions of Impala, to_date() returns column of type DATE
  expect_equal(
    tbl(impala, "flights") %>%
      transmute(date_part = as.Date(time_hour)) %>%
      arrange(date_part) %>%
      head(5) %>%
      collect() %>%
      pull(1) %>%
      as.character(),
    rep("2013-01-01", 5)
  )
})

test_that("as_date() returns date part of timestamp", {
  check_impala()
  # do not explicitly check type of returned column like in timestamp tests
  # because:
  # in older versions of Impala, to_date() returns column of type STRING
  # in newer versions of Impala, to_date() returns column of type DATE
  expect_equal(
    tbl(impala, "flights") %>%
      transmute(date_part = as_date(time_hour)) %>%
      arrange(date_part) %>%
      head(5) %>%
      collect() %>%
      pull(1) %>%
      as.character(),
    rep("2013-01-01", 5)
  )
})
