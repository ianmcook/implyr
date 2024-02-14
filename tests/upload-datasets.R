## This script uploads some of datasets required to pass package tests.
## Also required for tests are Lahman::batting and nycflights13::flights,
## however they are too large to consistently upload with copy_to.

library(nycflights13)
library(implyr)

## Script expects object `impala` (created by `src_impala`)

## If using Cloudera Impala JDBC drivers, turn off OptimizedInsert
## (example connection string):
## "jdbc:impala://127.0.0.1:21050;OptimizedInsert=0"
## This parameter pads strings to uniform size and will make some of the tests fail.

options(implyr.copy_to_size_limit = 10000000)

copy_to(
  impala,
  airlines,
  types=c("STRING", "STRING"),
  temporary = FALSE
)

copy_to(
  impala,
  mtcars,
  temporary = FALSE
)

copy_to(
  impala,
  data.frame(col1=c(1), col2=c("a")),
  "one_row",
  temporary = FALSE
)

copy_to(
  impala,
  iris %>% select(
    species=Species,
    sepal_length=Sepal.Length
  ),
  "iris",
  temporary = FALSE
)

copy_to(
  impala,
  data.frame(
    language="Spanish",
    test="El pingüino Wenceslao hizo kilómetros bajo exhaustiva lluvia y frío, añoraba a su querido cachorro"
  ),
  "unicode_test",
  temporary = FALSE,
  types=c("STRING", "STRING")
)
