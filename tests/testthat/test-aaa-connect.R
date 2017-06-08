context("connect")

test_that("can connect to Impala using RJDBC", {
  check_impala()
  connected <- tryCatch({
    jdbc_path <- tempdir()
    if (file.exists(jdbc_url)) {
      jdbc_zip <- jdbc_url
    } else {
      jdbc_zip <- file.path(jdbc_path, basename(jdbc_url))
      unlink(jdbc_zip)
      download.file(jdbc_url, jdbc_zip, quiet = TRUE)
    }
    jdbc_jars <- tools::file_path_sans_ext(jdbc_zip)
    unlink(jdbc_jars, recursive = TRUE)
    dir.create(jdbc_jars)
    unzip(jdbc_zip, exdir = jdbc_jars)
    impala_classpath <- list.files(path = jdbc_jars, pattern = "\\.jar$", full.names = TRUE)
    rJava::.jinit(classpath = impala_classpath)
    drv <- RJDBC::JDBC("com.cloudera.impala.jdbc41.Driver", impala_classpath, "`")
    jdbc_conn_str <- paste0("jdbc:impala://", jdbc_host, ":", jdbc_port)
    impala <<- implyr::src_impala(drv, jdbc_conn_str, jdbc_user, jdbc_pass)
    TRUE
  }, error = function(e) {
    FALSE
  })
  run_tests <<- connected
  expect_true(
    connected,
    info = "This environment is configured to run implyr tests, but the connection to Impala failed"
  )
})
