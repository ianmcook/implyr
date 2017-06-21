context("copy_to()")

test_that("copy_to() succeeds on a very small data frame", {
  check_impala()
  set.seed(seed = NULL)
  table_name <- paste0(sample(letters, 10, replace = TRUE), collapse = "")
  copy_to(impala, nycflights13::airlines[1:5, ], name = table_name, temporary = FALSE)
  airlines_impala <- dbGetQuery(impala, paste0("SELECT * FROM ", table_name, " ORDER BY carrier"))
  # right-trim variable length strings because RJDBC pads them with spaces
  airlines_impala$name <- gsub(" +$", "", airlines_impala$name)
  # clean up
  dbExecute(impala, paste0("DROP TABLE ", table_name))
  airlines_reference <- nycflights13::airlines %>% head(5) %>% arrange(carrier) %>% as.data.frame()
  compare_tbls(list(airlines_reference, airlines_impala), op = function(x) x)
})
