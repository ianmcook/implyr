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

#' Describe the Impala data source
#'
#' @name db_desc
#' @param x an object with class class \code{impala_connection}
#' @return A string containing information about the connection to Impala
#' @export
#' @importFrom dplyr db_desc
db_desc.impala_connection <- function(x) {
  info <- attr(x, "info")
  if (is.null(info)) {
    return("???")
  }
  info$version <-
    sub("\\s?.?(buil|release).*$", "", info$version, ignore.case = TRUE)
  if (!"url" %in% names(info)) {
    if (!is.null(info$host) && !is.null(info$port)) {
      info$url <- paste0(info$host, ":", info$port)
    } else if (!is.null(info$host)) {
      info$url <- info$host
    } else if (!is.null(info$dsn)) {
      info$url <- paste0(info$dsn)
    }
  } else {
    info$url <- paste0(info$url)
  }
  if (!is.null(info$user) && info$user != "") {
    info$user <- paste0(info$user, "@")
  }
  paste0(
    info$version,
    " through ",
    info$package,
    " [",
    info$user,
    info$url,
    "/",
    info$dbname,
    "]"
  )
}

#' @export
#' @importFrom dplyr sql_escape_ident
sql_escape_ident.impala_connection <- function(con, x) {
  sql_quote(x, "`")
}

#' @export
#' @importFrom dplyr sql_escape_string
sql_escape_string.impala_connection <- function(con, x) {
  sql_quote(x, "'")
}

#' @export
#' @importFrom dbplyr base_agg
#' @importFrom dbplyr base_scalar
#' @importFrom dbplyr base_win
#' @importFrom dbplyr build_sql
#' @importFrom dbplyr sql
#' @importFrom dbplyr sql_prefix
#' @importFrom dbplyr sql_translator
#' @importFrom dbplyr sql_variant
#' @importFrom dbplyr win_absent
#' @importFrom dbplyr win_current_group
#' @importFrom dbplyr win_over
#' @importFrom dplyr sql_translate_env
sql_translate_env.impala_connection <- function(con) {
  sql_variant(
    sql_translator(
      .parent = base_scalar,

      # type conversion functions
      as.character = function(x)
        build_sql("cast(", x, " as string)"),
      as.string = function(x)
        build_sql("cast(", x, " as string)"),
      as.char = function(x, len)
        build_sql("cast(", x, " as char(", as.integer(len), "))"),
      as.varchar = function(x, len)
        build_sql("cast(", x, " as varchar(", as.integer(len), "))"),
      as.boolean = function(x)
        build_sql("cast(", x, " as boolean)"),
      as.logical = function(x)
        build_sql("cast(", x, " as boolean)"),
      as.numeric = function(x)
        build_sql("cast(", x, " as double)"),
      as.int = function(x)
        build_sql("cast(", x, " as int)"),
      as.integer = function(x)
        build_sql("cast(", x, " as int)"),
      as.bigint = function(x)
        build_sql("cast(", x, " as bigint)"),
      as.smallint = function(x)
        build_sql("cast(", x, " as smallint)"),
      as.tinyint = function(x)
        build_sql("cast(", x, " as tinyint)"),
      as.double = function(x)
        build_sql("cast(", x, " as double)"),
      as.real = function(x)
        build_sql("cast(", x, " as real)"),
      as.float = function(x)
        build_sql("cast(", x, " as float)"),
      as.single = function(x)
        build_sql("cast(", x, " as float)"),
      as.decimal = function(x, pre = NULL, sca = NULL) {
        if (is.null(pre)) {
          build_sql("cast(", x, " as decimal)")
        } else {
          if (is.null(sca)) {
            build_sql("cast(", x, " as decimal(", as.integer(pre), "))")
          } else {
            build_sql("cast(",
                      x,
                      " as decimal(",
                      as.integer(pre),
                      ",",
                      as.integer(sca),
                      "))")
          }
        }
      },
      as.timestamp = function(x)
        build_sql("cast(", x, " as timestamp)"),

      # mathematical functions
      is.nan = sql_prefix("is_nan"),
      is.infinite = sql_prefix("is_inf"),
      is.finite = sql_prefix("!is_inf"),
      log = function(x, base = exp(1)) {
        if (base != exp(1)) {
          build_sql("log(", base, ", ", x, ")")
        } else {
          build_sql("ln(", x, ")")
        }
      },
      pmax = sql_prefix("greatest"),
      pmin = sql_prefix("least"),

      # date and time functions (work like lubridate)
      week = sql_prefix("weekofyear"),
      yday = sql_prefix("dayofyear"),
      mday = sql_prefix("day"),
      wday = function(x, label = FALSE, abbr = TRUE) {
        if (label) {
          if (abbr) {
            build_sql("substring(dayname(", x, "),1,3)")
          } else {
            build_sql("dayname(", x, ")")
          }
        } else {
          build_sql("dayofweek(", x, ")")
        }
      },

      # conditional functions
      na_if = sql_prefix("nullif", 2),

      # string functions
      paste = function(...,
                       sep = " ",
                       collapse = NULL) {
        if (is.null(collapse)) {
          sql(paste0(
            "concat_ws(",
            sql_escape_string(con, sep),
            ",",
            paste(list(...), collapse = ","),
            ")"
          ))
          # TBD: simplify this by passing con to build_sql?
        } else {
          stop("paste() with collapse argument set can only be used for aggregation",
               call. = FALSE)
        }
      },
      paste0 = function(..., collapse = NULL) {
        if (is.null(collapse)) {
          build_sql("concat(", sql(paste(list(...), collapse = ",")), ")")
        } else {
          stop("paste0() with collapse argument set can only be used for aggregation",
               call. = FALSE)
        }
      }
    ),
    sql_translator(
      .parent = base_agg,
      median = sql_prefix("appx_median"),
      n = function(x) {
        if (missing(x)) {
          sql("count(*)")
        } else {
          build_sql(sql("count"), list(x))
        }
      },
      sd =  sql_prefix("stddev"),
      unique = function(x) {
        sql(paste("distinct", x))
      },
      var = sql_prefix("variance"),
      paste = function(x, collapse = NULL) {
        if (is.null(collapse)) {
          stop("To use paste() as an aggregate function, set the collapse argument",
               call. = FALSE)
        } else {
          sql(paste0(
            "group_concat(",
            x,
            ",",
            sql_escape_string(con, collapse),
            ")"
          ))
        }
      },
      paste0 = function(x, collapse = NULL) {
        if (is.null(collapse)) {
          stop("To use paste0() as an aggregate function, set the collapse argument",
               call. = FALSE)
        } else {
          sql(paste0(
            "group_concat(",
            x,
            ",",
            sql_escape_string(con, collapse),
            ")"
          ))
        }
      }
    ),
    sql_translator(
      .parent = base_win,
      median = win_absent("median"),
      n = function(x) {
        if (missing(x)) {
          win_over(sql("count(*)"),
                   partition = win_current_group())
        } else {
          win_over(build_sql(sql("count"), list(x)),
                   partition = win_current_group())
        }
      },
      n_distinct = win_absent("n_distinct"),
      ndv = win_absent("ndv"),
      paste = function(...,
                       sep = " ",
                       collapse = NULL) {
        if (is.null(collapse)) {
          sql(paste0(
            "concat_ws(",
            sql_escape_string(con, sep),
            ",",
            paste(list(...), collapse = ","),
            ")"
          ))
          # TBD: simplify this by passing con to build_sql?
        } else {
          stop("paste() with collapse argument is not supported in window functions",
               call. = FALSE)
        }
      },
      paste0 = function(...,  collapse = NULL) {
        if (is.null(collapse)) {
          build_sql("concat(", sql(paste(list(...), collapse = ",")), ")")
        } else {
          stop("paste0() with collapse argument is not supported in window functions",
               call. = FALSE)
        }
      },
      sd = win_absent("sd"),
      unique = function(x) {
        sql(paste("distinct", x))
      },
      var = win_absent("var")
    )
  )
}

#' @export
#' @importFrom assertthat assert_that
#' @importFrom assertthat is.string
#' @importFrom assertthat is.flag
#' @importFrom dbplyr ident
#' @importFrom dplyr db_save_query
db_save_query.impala_connection <-
  function(con,
           sql,
           name,
           temporary = TRUE,
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
    is.flag(force),
    is.flag(analyze),
    is_string_or_null(file_format),
    is_nchar_one_string_or_null(field_terminator),
    is_nchar_one_string_or_null(line_terminator)
  )
  if (temporary) {
    stop(
      "Impala does not support temporary tables. Set temporary = FALSE in db_save_query().",
      call. = FALSE
    )
  }

  # too dangerous
  #if (overwrite) {
  #  db_drop_table(con, name, force = TRUE)
  #}

  tt_sql <- build_sql("CREATE ",
                      if (external) {
                        sql("EXTERNAL ")
                      },
                      "TABLE ",
                      if (force) {
                        sql("IF NOT EXISTS ")
                      },
                      ident(name),
                      " ",
                      if (!is.null(field_terminator) ||
                          !is.null(line_terminator)) {
                        sql("ROW FORMAT DELIMITED ")
                      },
                      if (!is.null(field_terminator)) {
                        sql(paste0("FIELDS TERMINATED BY \"", field_terminator, "\" "))
                      },
                      if (!is.null(line_terminator)) {
                        sql(paste0("LINES TERMINATED BY \"", line_terminator, "\" "))
                      },
                      if (!is.null(file_format)) {
                        sql(paste0("STORED AS ", file_format, " "))
                      },
                      "AS ",
                      sql,
                      con = con)
  if (analyze) {
    db_analyze(con, name)
  }
  dbExecute(con, tt_sql)
  name
}


#' @export
#' @importFrom dplyr db_begin
db_begin.impala_connection <- function(con, ...) {
  # do nothing
}

#' @export
#' @importFrom dplyr db_commit
db_commit.impala_connection <- function(con, ...) {
  # do nothing
}

#' @export
#' @importFrom dplyr db_analyze
db_analyze.impala_connection <- function(con, table, ...) {
  sql <- build_sql("COMPUTE STATS", ident(table), con = con)
  dbExecute(con, sql)
}

#' @export
#' @importFrom dplyr db_drop_table
db_drop_table.impala_connection <-
  function(con,
           table,
           force = FALSE,
           purge = FALSE,
           ...) {
  sql <- build_sql("DROP TABLE ",
                   if (force) {
                     sql("IF EXISTS ")
                   },
                   ident(table),
                   if (purge) {
                     sql(" PURGE")
                   },
                   con = con)
  dbExecute(con, sql)
}

#' @export
#' @importFrom assertthat assert_that
#' @importFrom assertthat is.string
#' @importFrom assertthat is.flag
#' @importFrom dbplyr escape
#' @importFrom dplyr db_insert_into
db_insert_into.impala_connection <-
  function(con, table, values, overwrite = FALSE, ...) {
  assert_that(is.string(table),
              is.data.frame(values),
              is.flag(overwrite))
  if (nrow(values) == 0) {
    return(NULL)
  }

  cols <-
    lapply(values,
           escape,
           collapse = NULL,
           parens = FALSE,
           con = con)
  col_mat <-
    matrix(unlist(cols, use.names = FALSE), nrow = nrow(values))

  rows <- apply(col_mat, 1, paste0, collapse = ", ")
  values <- paste0("(", rows, ")", collapse = "\n, ")

  sql <- build_sql("INSERT ",
                   if (overwrite) {
                     sql("OVERWRITE ")
                   } else {
                     sql("INTO ")
                   },
                   ident(table),
                   " VALUES ",
                   sql(values),
                   con = con)
  dbExecute(con, sql)
}

#' @export
#' @importFrom dplyr db_data_type
db_data_type.impala_connection <- function(con, fields, ...) {
  data_type <- function(x) {
    switch(
      class(x)[1],
      logical =   "boolean",
      integer =   "int",
      numeric =   "double",
      factor =    "string",
      character = "string",
      Date =      "timestamp",
      POSIXct =   "timestamp",
      stop("Unknown class ", paste(class(x), collapse = "/"), call. = FALSE)
    )
  }
  vapply(fields, data_type, FUN.VALUE = character(1))
}

#' @export
#' @importFrom assertthat assert_that
#' @importFrom assertthat is.string
#' @importFrom assertthat is.flag
#' @importFrom dbplyr escape
#' @importFrom dbplyr ident
#' @importFrom dbplyr sql_vector
#' @importFrom dplyr db_create_table
db_create_table.impala_connection <-
  function (con,
            table,
            types,
            temporary = FALSE,
            external = FALSE,
            force = FALSE,
            field_terminator = NULL,
            line_terminator = NULL,
            file_format = NULL,
            ...) {
  # TBD: add params to control location and other CREATE TABLE options

  assert_that(
    is.string(table),
    is.character(types),
    is.flag(temporary),
    is_string_or_null(file_format),
    is_nchar_one_string_or_null(field_terminator),
    is_nchar_one_string_or_null(line_terminator)
  )
  if (temporary) {
    stop(
      "Impala does not support temporary tables. Set temporary = FALSE in db_create_table().",
      call. = FALSE
    )
  }
  field_names <-
    escape(ident(names(types)), collapse = NULL, con = con)
  fields <- sql_vector(
    paste0(field_names, " ", types),
    parens = TRUE,
    collapse = ", ",
    con = con
  )
  sql <- build_sql("CREATE ",
                   if (external) {
                     sql("EXTERNAL ")
                   },
                   "TABLE ",
                   if (force) {
                     sql("IF NOT EXISTS ")
                   },
                   ident(table),
                   " ",
                   if (!is.null(field_terminator) ||
                       !is.null(line_terminator)) {
                     sql("ROW FORMAT DELIMITED ")
                   },
                   if (!is.null(field_terminator)) {
                     sql(paste0("FIELDS TERMINATED BY \"", field_terminator, "\" "))
                   },
                   if (!is.null(line_terminator)) {
                     sql(paste0("LINES TERMINATED BY \"", line_terminator, "\" "))
                   },
                   if (!is.null(file_format)) {
                     sql(paste0("STORED AS ", file_format, " "))
                   },
                   fields,
                   con = con)
  dbExecute(con, sql)
}

