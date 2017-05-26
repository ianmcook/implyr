context("disconnect")

test_that("can disconnect from Impala using RJDBC", {
  check_impala()
  dbDisconnect(impala)
  succeed()
})
