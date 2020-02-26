context("rowwise translations")

test_that("result of rowwise mutate() is consistent with result on tbl_df", {
  skip("Does not currently work")
  check_impala()
  test_op <- function(x) {
    x %>%
      rename_all(tolower) %>%
      rename_all(.funs = list(~ gsub(".", "_", ., fixed = TRUE))) %>%
      select(-species) %>%
      rowwise() %>%
      mutate(m = min(sepal_length, sepal_width, petal_length, petal_width))
  }
  compare_tbls(list(tbl(impala, "iris"), datasets::iris), op = test_op, convert = TRUE)
})
