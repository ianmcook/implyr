context("compute()")

test_that("compute(temporary = FALSE) succeeds", {
  skip_for_codecov()
  check_impala()
  set.seed(seed = NULL)
  table_name <- paste0(sample(letters, 10, replace = TRUE), collapse = "")
  dbExecute(impala, paste0("DROP TABLE IF EXISTS ", table_name))
  Sys.sleep(2)
  iris_unlabeled_impala <-
    tbl(impala, "iris") %>%
    select(-species) %>%
    compute(table_name, temporary = FALSE)
  iris_unlabeled_impala <-
    iris_unlabeled_impala %>%
    arrange(sepal_length, sepal_width, petal_length, petal_width) %>%
    collect() %>%
    mutate_if(is.numeric, round, 2)
  # clean up
  Sys.sleep(2)
  dbExecute(impala, paste0("DROP TABLE IF EXISTS ", table_name))
  iris_unlabeled_reference <-
    tbl(impala, "iris") %>%
    select(-species) %>%
    arrange(sepal_length, sepal_width, petal_length, petal_width) %>%
    collect() %>%
    mutate_if(is.numeric, round, 2)
  compare_tbls(
    list(iris_unlabeled_impala, iris_unlabeled_reference),
    op = function(x) x
  )
})
