# Copyright 2018 Cloudera Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# register virtual class
#' @importFrom methods setOldClass
setOldClass("tbl_impala")

#' @export
#' @importFrom dplyr intersect
intersect.tbl_impala <- function(x, y, copy = FALSE, ...) {
  stop("Impala does not support intersect operations.", call. = FALSE)
}

#' @export
#' @importFrom dplyr setdiff
setdiff.tbl_impala <- function(x, y, copy = FALSE, ...) {
  stop("Impala does not support setdiff operations.", call. = FALSE)
}

#' Force execution of an Impala query
#'
#' @name compute
#' @description \describe{ \item{\code{compute()}}{Executes the query and stores
#' the result in a new Impala table} \item{\code{collect()}}{Executes the query
#' and returns the result to R as a data frame \code{tbl}}
#' \item{\code{collapse()}}{Generates the query for later execution} }
#'
#' @param x an object with class \code{tbl_impala}
#' @param name the name for the new Impala table
#' @param temporary must be set to \code{FALSE}
#' @param unique_indexes not used
#' @param indexes not used
#' @param analyze whether to run \code{COMPUTE STATS} after adding data to the
#'   new table
#' @param external whether the new table will be externally managed
#' @param overwrite whether to overwrite existing table data (currently ignored)
#' @param force whether to silently fail if the table already exists
#' @param field_terminator the deliminter to use between fields in text file
#'   data. Defaults to the ASCII control-A (hex 01) character
#' @param line_terminator the line terminator. Defaults to \code{"\n"}
#' @param file_format the storage format to use. Options are \code{"TEXTFILE"}
#'   (default) and \code{"PARQUET"}
#' @param ... other arguments passed on to methods
#' @note Impala does not support temporary tables. When using \code{compute()}
#'   to store results in an Impala table, you must set \code{temporary = FALSE}.
#' @export
#' @importFrom assertthat assert_that
#' @importFrom assertthat is.string
#' @importFrom assertthat is.flag
#' @importFrom dbplyr op_grps
#' @importFrom dbplyr op_vars
#' @importFrom dplyr %>%
#' @importFrom dplyr compute
#' @importFrom dplyr group_by
#' @importFrom dplyr select
#' @importFrom dplyr tbl
#' @importFrom rlang !!!
#' @importFrom rlang syms
#' @importFrom utils getFromNamespace
compute.tbl_impala <-
  function(x,
           name,
           temporary = TRUE,
           unique_indexes = NULL,
           indexes = NULL,
           analyze = FALSE,
           external = FALSE,
           overwrite = FALSE,
           force = FALSE,
           field_terminator = NULL,
           line_terminator = NULL,
           file_format = NULL,
           ...) {
  # TBD: add params to control location and other CREATE TABLE options

  assert_that(
    is.string(name),
    is.flag(temporary),
    is.flag(external),
    is.flag(overwrite),
    is.flag(force),
    is.flag(analyze),
    is_string_or_null(file_format),
    is_nchar_one_string_or_null(field_terminator),
    is_nchar_one_string_or_null(line_terminator)
  )
  if (temporary) {
    stop(
      "Impala does not support temporary tables. Set temporary = FALSE in compute().",
      call. = FALSE
    )
  }

  vars <- op_vars(x)
  assert_that(all(unlist(indexes) %in% vars))
  assert_that(all(unlist(unique_indexes) %in% vars))

  x_aliased <- select(x, !!! syms(vars))
  sql <- db_sql_render(x$src$con, x_aliased$ops)

  # TBD: implement db_compute.impala_connection and call it here instead of db_save_query
  name <- db_save_query(
    con = x$src$con,
    sql = sql,
    name = name,
    temporary = FALSE,
    analyze = analyze,
    external = external,
    overwrite = overwrite,
    force = force,
    field_terminator = field_terminator,
    line_terminator = field_terminator,
    file_format = file_format,
    ...
  )

  tbl(x$src, name) %>%
    group_by(!!! syms(op_grps(x)))
}

#' @name collect
#' @rdname compute
#' @param n the number of rows to return
#' @param warn_incomplete whether to issue a warning if not all rows retrieved
#' @export
#' @importFrom dplyr collect
collect.tbl_impala <-
  function(x,
           ...,
           n = Inf,
           warn_incomplete = TRUE) {
    NextMethod("collect")
  }

#' @name collapse
#' @rdname compute
#' @param vars not used
#' @export
#' @importFrom dplyr collapse
collapse.tbl_impala <- function(x, vars = NULL, ...) {
  NextMethod("collapse")
}

