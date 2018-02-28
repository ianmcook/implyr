context("complex columns")

test_that("impala_unnest() on array column returns expected result", {
  check_impala()
  test_op <- function(x) {
    x %>%
      impala_unnest(phones) %>%
      mutate(phones_pos = as.numeric(phones_pos)) %>%
      arrange(cust_id, phones_pos) %>%
      as.data.frame(stringsAsFactors=FALSE)
  }
  ref <- data.frame(
    cust_id = c("a", "a", "a", "b", "c", "c"),
    name = c("Alice", "Alice", "Alice", "Bob", "Carlos", "Carlos"),
    phones_item = c("555-1111", "555-2222", "555-3333", "555-4444", "555-5555", "555-6666"),
    phones_pos = c(0, 1 , 2, 0, 0, 1),
    stringsAsFactors = FALSE
  )
  compare(test_op(tbl(impala, "cust_phones_parquet")), ref)
})

test_that("impala_unnest() on map column returns expected result", {
  check_impala()
  test_op <- function(x) {
    x %>%
      impala_unnest(phones) %>%
      arrange(cust_id, phones_value) %>%
      as.data.frame(stringsAsFactors=FALSE)
  }
  ref <- data.frame(
    cust_id = c("a", "a", "a", "b", "c", "c"),
    name = c("Alice", "Alice", "Alice", "Bob", "Carlos", "Carlos"),
    phones_key = c("home", "work", "mobile", "mobile", "work", "home"),
    phones_value = c("555-1111", "555-2222", "555-3333", "555-4444", "555-5555", "555-6666"),
    stringsAsFactors = FALSE
  )
  compare(test_op(tbl(impala, "cust_phones_map_parquet")), ref)
})

test_that("impala_unnest() on struct column returns expected result", {
  check_impala()
  test_op <- function(x) {
    x %>%
      impala_unnest(address) %>%
      arrange(cust_id) %>%
      as.data.frame(stringsAsFactors=FALSE)
  }
  ref <- data.frame(
    cust_id = c("a", "b", "c"),
    name = c("Alice", "Bob", "Carlos"),
    address_street = c("742 Evergreen Terrace", "1600 Pennsylvania Ave NW", "342 Gravelpit Terrace"),
    address_city = c("Springfield", "Washington", "Bedrock"),
    address_state = c("OR", "DC", NA),
    address_zipcode = c("97477", "20500", NA),
    stringsAsFactors = FALSE
  )
  compare(test_op(tbl(impala, "cust_addr_parquet")), ref)
})
