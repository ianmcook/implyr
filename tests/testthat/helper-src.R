library(RJDBC)
library(nycflights13)

java_home <- Sys.getenv("JAVA_HOME", unset = NA)
jdbc_url <- Sys.getenv("IMPLYR_TEST_JDBC", unset = NA)
jdbc_host <- Sys.getenv("IMPLYR_TEST_HOST", unset = NA)
jdbc_port <- Sys.getenv("IMPLYR_TEST_PORT", unset = NA)
jdbc_user <- Sys.getenv("IMPLYR_TEST_USER", unset = NA)
jdbc_pass <- Sys.getenv("IMPLYR_TEST_PASS", unset = NA)

run_tests <- FALSE
if (!is.na(java_home) & !is.na(jdbc_url) & !is.na(jdbc_host) &
    !is.na(jdbc_port) & !is.na(jdbc_user) & !is.na(jdbc_pass)) {
  jdbc_path <- tempdir()
  jdbc_zip <- file.path(jdbc_path, basename(jdbc_url))
  unlink(jdbc_zip)
  download.file(jdbc_url, jdbc_zip)
  jdbc_jars <- tools::file_path_sans_ext(jdbc_zip)
  unlink(jdbc_jars, recursive = TRUE)
  dir.create(jdbc_jars)
  unzip(jdbc_zip, exdir = jdbc_jars)
  impala_classpath <- list.files(path = jdbc_jars, pattern = "\\.jar$", full.names = TRUE)
  rJava::.jinit(classpath = impala_classpath)
  drv <- RJDBC::JDBC("com.cloudera.impala.jdbc41.Driver", impala_classpath, "`")
  jdbc_conn_str <- paste0("jdbc:impala://", jdbc_host, ":", jdbc_port)
  impala <- implyr::src_impala(drv, jdbc_conn_str, jdbc_user, jdbc_pass)
  run_tests <- TRUE
}

check_impala <- function() {
  if (!run_tests) {
    skip("This environment not configured for tests")
  }
}

disconnect <- function() {
  if (run_tests) {
    dbDisconnect(impala)
  }
}
