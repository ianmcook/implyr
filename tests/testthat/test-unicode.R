context("Unicode characters")

test_that("Sample Spanish language string is returned correctly", {
  check_impala()
  expect_equal(
    tbl(impala, "unicode_test") %>%
      filter(language == "Spanish") %>%
      select(test) %>%
      collect() %>%
      pull(1) %>%
      as.character(),
    "El pingüino Wenceslao hizo kilómetros bajo exhaustiva lluvia y frío, añoraba a su querido cachorro"
  )
})
