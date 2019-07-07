# Copyright 2019 Cloudera Inc.
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

#' Unnest a complex column in an Impala table
#'
#' @description \describe{\code{impala_unnest()}}{ unnests a
#' column of type \code{ARRAY}, \code{MAP}, or \code{STRUCT}
#' in a \code{tbl_impala}. These column types are referred to
#' as complex or nested types.}
#'
#' @param data an object with class \code{tbl_impala}
#' @param col the unquoted name of an \code{ARRAY},
#' \code{MAP}, or \code{STRUCT} column
#' @param ... ignored (included for compatibility)
#' @return an object with class \code{tbl_impala} with the
#' complex column unnested into two or more separate columns
#' @details \code{impala_unnest()} currently can unnest only
#' one column, can only be applied once to a \code{tbl_impala},
#' and must be applied to a \code{tbl_impala} representing an
#' Impala table or view before applying any other operations.
#' @seealso \href{https://www.cloudera.com/documentation/enterprise/latest/topics/impala_complex_types.html}{
#' Impala Complex Types}
#' @export
#' @importFrom dbplyr ident_q
#' @importFrom rlang enexpr
#' @importFrom rlang !!
#' @importFrom rlang :=
#' @importFrom tidyselect vars_select
#' @importFrom tidyselect vars_rename
impala_unnest <- function(data, col, ...) {
  res <- data
  if (!inherits(res, "tbl_impala")) {
    stop("data argument must be a tbl_impala", call. = FALSE)
  }
  if (!"vars" %in% names(res$ops$x) ||
      all(is.na(attr(res$ops$x$vars, "complex_type")))) {
    stop("data argument must contain complex columns", call. = FALSE)
  }
  if (is.null(attr(res$ops$x$vars, "complex_type"))) {
    stop("impala_unnest() can only be applied once to a tbl_impala",
          call. = FALSE)
  }
  if (!"x" %in% names(res$ops$x) ||
      !inherits(res$ops$x$x, "ident")) {
    stop("impala_unnest() must be applied to a tbl_impala before any other operations",
         call. = FALSE)
  }
  col <- enexpr(col)
  colname <- as.character(col)
  if (length(colname) != 1) {
    stop("impala_unnest() can unnest only one column")
  }
  colindex <- which(res$ops$x$vars == colname)
  if (length(colindex) != 1) {
    stop("Column ", colname, " not found", call. = FALSE)
  }
  coltype <- attr(res$ops$x$vars, "complex_type")[colindex]
  tablename <- as.character(res$ops$x$x)
  if (identical(coltype, "array")) {
    quoted_tablename <- impala_escape_ident(res$src$con, tablename, "`")
    res$ops$x$x <- ident_q(
      paste0(quoted_tablename, ", ", quoted_tablename, ".`", colname,"`")
    )
    res$ops$x$vars <- c(
      setdiff(res$ops$x$vars, colname),
      paste0(colname, ".item"),
      paste0(colname, ".pos")
    )
    res$ops <- res$ops$x
    item_name_before <- paste0(colname, ".item")
    item_name_after <- paste0(colname, "_item")
    pos_name_before <- paste0(colname, ".pos")
    pos_name_after <- paste0(colname, "_pos")
    rename_complex_cols <- vars_rename(
      colnames(res),
      !!item_name_after := !!item_name_before,
      !!pos_name_after := !!pos_name_before
    )
    res <- select(res, rename_complex_cols)
  } else if (identical(coltype, "map")) {
    quoted_tablename <- impala_escape_ident(res$src$con, tablename, "`")
    res$ops$x$x <- ident_q(
      paste0(quoted_tablename, ", ", quoted_tablename, ".`", colname,"`")
    )
    res$ops$x$vars <- c(
      setdiff(res$ops$x$vars, colname),
      paste0(colname, ".key"),
      paste0(colname, ".value")
    )
    res$ops <- res$ops$x
    key_name_before <- paste0(colname, ".key")
    key_name_after <- paste0(colname, "_key")
    value_name_before <- paste0(colname, ".value")
    value_name_after <- paste0(colname, "_value")
    rename_complex_cols <- vars_rename(
      colnames(res),
      !!key_name_after := !!key_name_before,
      !!value_name_after := !!value_name_before
    )
    res <- select(res, rename_complex_cols)
  } else if (identical(coltype, "struct")) {
    sql <- paste0("DESCRIBE ", tablename, ".", colname)
    structcolnames <- dbGetQuery(res$src, sql)$name
    othercolnames <- setdiff(res$ops$x$vars, colname)
    col_names_before <- c(
      othercolnames,
      paste(colname, structcolnames, sep = ".")
    )
    col_names_after <- c(
      othercolnames,
      paste(colname, structcolnames, sep = "_")
    )
    res$ops$x$vars <- col_names_before
    res$ops <- res$ops$x
    rename_complex_cols <- col_names_before
    names(rename_complex_cols) <- col_names_after
    res <- select(res, !!rename_complex_cols)
  } else {
    stop("Column ", colname, " must be of type ARRAY, MAP, or STRUCT", call. = FALSE)
  }
  res
}
