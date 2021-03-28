java_home <- Sys.getenv("JAVA_HOME", unset = NA)
jdbc_url <- Sys.getenv("IMPLYR_TEST_JDBC", unset = NA)
jdbc_host <- Sys.getenv("IMPLYR_TEST_HOST", unset = NA)
jdbc_port <- Sys.getenv("IMPLYR_TEST_PORT", unset = NA)
jdbc_user <- Sys.getenv("IMPLYR_TEST_USER", unset = NA)
jdbc_pass <- Sys.getenv("IMPLYR_TEST_PASS", unset = NA)

if (!is.na(java_home) & !is.na(jdbc_url) & !is.na(jdbc_host) &
    !is.na(jdbc_port) & !is.na(jdbc_user) & !is.na(jdbc_pass)) {
  run_tests <- TRUE
} else {
  run_tests <- FALSE
}

check_impala <- function() {
  if (!run_tests) {
    skip("This environment is not configured to run implyr tests")
  }
}

skip_on_travis <- function() {
  if (identical(Sys.getenv("TRAVIS"), "true")) {
    skip("Skipping test on Travis CI")
  }
}

skip_for_codecov <- function() {
  if (identical(Sys.getenv("R_COVR"), "true")) {
    skip("Skipping for on Codecov")
  }
}

column_type <- function(x) {
  if (ncol(x) > 1) {
    stop("Argument to function column_type() must have only one column")
  }
  x %>%
    select(test_column = 1) %>%
    transmute(datatype = typeof(test_column)) %>%
    head(1) %>%
    pull() %>%
    tolower()
}

eval_tbls <- function (tbls, op)
{
  # adapted from dplyr::eval_tbls
  lapply(tbls, function(x) as.data.frame(op(x)))
}

eval_tbls2 <- function(tbls_x, tbls_y, op)
{
  # adapted from dplyr::eval_tbls2
  Map(function(x, y) as.data.frame(op(x, y)), tbls_x, tbls_y)
}

expect_equal_tbls <- function (results, ref = NULL, ...)
{
  # adapted from dplyr::expect_equal_tbls
  if (length(results) < 2 && is.null(ref)) {
    skip("Need at least two srcs to compare")
  }
  if (is.null(ref)) {
    ref <- results[[1]]
    ref_name <- names(results)[1]
    rest <- results[-1]
  }
  else {
    rest <- results
    ref_name <- "supplied comparison"
  }
  for (i in seq_along(rest)) {
    ok <- all.equal(ref, rest[[i]], ...)
    msg <- paste0(names(rest)[[i]], " not equal to ", ref_name,
                  "\n", ok)
    expect_true(ok, info = msg)
  }
  invisible(TRUE)
}

compare <- all.equal

compare_tbls <- function (tbls, op, ref = NULL, ...)
{
  # adapted from dplyr::compare_tbls
  results <- eval_tbls(tbls, op)
  expect_equal_tbls(results, ...)
}

compare_tbls2 <- function (tbls_x, tbls_y, op, ref = NULL, compare = compare, ...)
{
  # adapted from dplyr::compare_tbls2
  results <- eval_tbls2(tbls_x, tbls_y, op)
  expect_equal_tbls(results, ...)
}
