context("Conditional function translations")

test_that("Expression in square brackets in summarise() returns expected result", {
  check_impala()
  test_op <- function(x) {
    x %>%
      rename_all(recode, Sepal.Length = "sepal_length") %>%
      rename_all(recode, Species = "species") %>%
      summarise(result = ceiling(sum(sepal_length[species == "versicolor"], na.rm = TRUE))) %>%
      collect() %>%
      pull(1) %>%
      as.numeric()
  }
  compare_tbls(list(tbl(impala, "iris"), datasets::iris), op = test_op)
})

test_that("na_if() returns expected result", {
  check_impala()
  test_op <- function(x) {
    x %>%
      rename_all(recode, Species = "species") %>%
      transmute(mangled = na_if(species, "virginica")) %>%
      collect() %>%
      arrange(mangled)
      as.character()
  }
  compare_tbls(list(tbl(impala, "iris"), datasets::iris), op = test_op)
})

