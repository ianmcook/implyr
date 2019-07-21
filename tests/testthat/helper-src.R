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
