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
#' @importFrom dplyr db_query_fields
#' @importFrom dplyr sql_select
#' @importFrom dplyr sql_subquery
#' @importFrom dbplyr sql
#' @importFrom DBI dbSendQuery
#' @importFrom DBI dbClearResult
#' @importFrom DBI dbFetch
db_query_fields.impala_connection <- function(con, sql, ...) {
  # if the argument "sql" is an identifier, it will not contain whitespace
  # and if not, then it will contain whitespace
  if (grepl("\\s", sql)) {
    # get column names with SELECT ... WHERE FALSE
    sql <- sql_select(con, sql("*"), sql_subquery(con, sql), where = sql("FALSE"))
    qry <- dbSendQuery(con, sql)
    on.exit(dbClearResult(qry))
    res <- dbFetch(qry, 0)
    names(res)
  } else {
    # get column names with DESCRIBE
    sql <- paste("DESCRIBE", sql)
    res <- dbGetQuery(con, sql)
    # attribute "complex" represents whether each column has complex type
    is_complex <- grepl("^\\s*(array|map|struct)<", res$type, ignore.case = TRUE)
    if (any(is_complex)) {
      complex_type <- rep_len(as.character(NA), length(res$name))
      complex_type[grepl("^\\s*array<", res$type, ignore.case = TRUE)] <- "array"
      complex_type[grepl("^\\s*map<", res$type, ignore.case = TRUE)] <- "map"
      complex_type[grepl("^\\s*struct<", res$type, ignore.case = TRUE)] <- "struct"
      attr(res$name, "complex_type") <- complex_type
    }
    res$name
  }
}

#' @export
#' @importFrom dplyr sql_escape_ident
sql_escape_ident.impala_connection <- function(con, x) {
  impala_escape_ident(con, x, "`")
}

#' @export
#' @importFrom dplyr sql_escape_string
sql_escape_string.impala_connection <- function(con, x) {
  sql_quote(x, "'")
}

impala_escape_ident <- function(con, x, quote) {
  if (length(x) == 0) {
    return(x)
  }

  y <- strsplit(x, ".", fixed = TRUE)
  y <- vapply(X = y, FUN = function(yi) {
    yi <- gsub(quote, paste0(quote, quote), yi, fixed = TRUE)
    yi <- paste0(quote, yi, quote)
    out <- paste(yi, collapse = ".")
    if (any(grepl("(", as.character(out), fixed = TRUE))) {
      return(gsub(paste0(quote, ".", quote), ".", out))
    }
    out
  }, FUN.VALUE = character(1))
  y[is.na(x)] <- "NULL"
  names(y) <- names(x)

  y
}

#' @export
#' @importFrom dbplyr base_agg
#' @importFrom dbplyr base_scalar
#' @importFrom dbplyr base_win
#' @importFrom dbplyr build_sql
#' @importFrom dbplyr sql
#' @importFrom dbplyr sql_expr
#' @importFrom dbplyr sql_aggregate
#' @importFrom dbplyr sql_prefix
#' @importFrom dbplyr sql_translator
#' @importFrom dbplyr sql_variant
#' @importFrom dbplyr win_absent
#' @importFrom dbplyr win_aggregate
#' @importFrom dbplyr win_current_group
#' @importFrom dbplyr win_over
#' @importFrom dplyr sql_translate_env
#' @importFrom rlang !!
#' @importFrom rlang !!!
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
      as.datetime = function(x)
        build_sql("cast(", x, " as timestamp)"),
      as_datetime = function(x)
        build_sql("cast(", x, " as timestamp)"),
      as.Date = function(x)
        build_sql("to_date(", x, ")"),
      as_date = function(x)
        build_sql("to_date(", x, ")"),
      as.POSIXct = function(x)
        build_sql("cast(", x, " as timestamp)"),

      # mathematical functions
      is.nan = sql_prefix("is_nan", 1),
      is.infinite = sql_prefix("is_inf", 1),
      is.finite = sql_prefix("!is_inf", 1),
      log = function(x, base = exp(1)) {
        if (base != exp(1)) {
          build_sql("log(", base, ", ", x, ")")
        } else {
          build_sql("ln(", x, ")")
        }
      },
      pmax = sql_prefix("greatest"),
      pmin = sql_prefix("least"),

      # lubridate functions
      year = sql_prefix("year", 1),
      month = function(x, label = FALSE) {
        if (label) {
          build_sql("from_unixtime(unix_timestamp(", x, "), 'MMM')")
        } else {
          build_sql("month(", x, ")")
        }
      },
      isoweek = sql_prefix("weekofyear", 1),
      yday = sql_prefix("dayofyear", 1),
      day = sql_prefix("day", 1),
      mday = sql_prefix("day", 1),
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
      hour = sql_prefix("hour", 1),
      minute = sql_prefix("minute", 1),
      second = sql_prefix("second", 1),
      today = function()
        build_sql("to_date(now())"),
      now = sql_prefix("now", 0),
      floor_date = function(x, unit = "second")
        build_sql("date_trunc(", unit, ", ", x, ")"),

      # conditional functions
      na_if = sql_prefix("nullif", 2),

      # string functions
      paste = function(..., sep = " ", collapse = NULL) {
        if (is.null(collapse)) {
          sql_expr(concat_ws(!!sep, !!!list(...)))
        } else {
          stop("paste() with collapse argument set can only be used for aggregation",
               call. = FALSE)
        }
      },
      paste0 = function(..., collapse = NULL) {
        if (is.null(collapse)) {
          sql_expr(concat(!!!list(...)))
        } else {
          stop("paste0() with collapse argument set can only be used for aggregation",
               call. = FALSE)
        }
      },
      nchar = sql_prefix("length", 1),
      trim = sql_prefix("trim"),
      trimws = function(string, side = c("both", "left", "right")) {
        side <- match.arg(side)
        switch(
          side,
          left = sql_expr(ltrim(!!string)),
          right = sql_expr(rtrim(!!string)),
          both = sql_expr(trim(!!string))
        )
      },
      toupper = sql_prefix("upper", 1),
      tolower = sql_prefix("lower", 1),
      rev = sql_prefix("reverse", 1),
      substr = function(x, start, stop) {
        start <- as.integer(start)
        length <- pmax(as.integer(stop) - start + 1L, 0L)
        build_sql(sql("substr"), list(x, start, length))
      },

      # stringr functions
      str_c = function(..., sep = "", collapse = NULL) {
        if (is.null(collapse)) {
          sql_expr(concat_ws(!!sep, !!!list(...)))
        } else {
          stop("str_c() with collapse argument set can only be used for aggregation",
               call. = FALSE)
        }
      },
      str_length = sql_prefix("length", 1),
      str_trim = function(string, side = c("both", "left", "right")) {
        side <- match.arg(side)
        switch(
          side,
          left = sql_expr(ltrim(!!string)),
          right = sql_expr(rtrim(!!string)),
          both = sql_expr(trim(!!string))
        )
      },
      str_to_lower = sql_prefix("lower", 1),
      str_to_upper = sql_prefix("upper", 1),
      str_to_title = sql_prefix("initcap", 1),
      str_sub = function(string, start = 1L, end = -1L) {
        stopifnot(length(start) == 1L, length(end) == 1L)
        start <- as.integer(start)
        end <- as.integer(end)
        if (end == -1L) {
          build_sql(sql("substr"), list(string, start))
        } else if (end < 0) {
          if (start < 0) {
            length <- pmax(-start + end + 1L, 0L)
          } else {
            length <- sql_expr(length(!!string) - !!start - !!(abs(end)) + 2L)
          }
          build_sql(sql("substr"), list(string, start, length))
        } else if (end > 0) {
          if (start < 0) {
            length <- sql_expr(length(!!string) - !!(abs(start)) + !!end - 2L)
          } else {
            length <- pmax(end - start + 1L, 0L)
          }
          build_sql(sql("substr"), list(string, start, length))
        } else if (end == 0) {
          build_sql(sql("substr"), list(string, start, 0L))
        }
      },

      # regular expression functions
      grepl = function(pattern, x, ignore.case = FALSE) {
        if (identical(ignore.case, TRUE)) {
          build_sql(x, sql(" IREGEXP "), pattern)
        } else {
          build_sql(x, sql(" REGEXP "), pattern)
        }
      },
      gsub = function(pattern, replacement, x) {
        build_sql(sql("regexp_replace"), list(x, pattern, replacement))
      },

      # bitwise functions
      bitwNot = sql_prefix("bitnot", 1),
      bitwAnd = sql_prefix("bitand", 2),
      bitwOr = sql_prefix("bitor", 2),
      bitwXor = sql_prefix("bitxor", 2),
      bitwShiftL = sql_prefix("shiftleft", 2),
      bitwShiftR = sql_prefix("shiftright", 2)

    ),
    sql_translator(
      .parent = base_agg,
      appx_median = sql_aggregate("appx_median"),
      avg = sql_aggregate("avg"),
      median = sql_aggregate_compat("appx_median", "median"),
      n = function(x) {
        if (missing(x)) {
          sql("count(*)")
        } else {
          build_sql(sql("count"), list(x))
        }
      },
      ndv = sql_aggregate("ndv"),
      sd = sql_aggregate_compat("stddev", "sd"),
      stddev = sql_aggregate("stddev"),
      stddev_samp = sql_aggregate("stddev_samp"),
      stddev_pop = sql_aggregate("stddev_pop"),
      unique = function(x) {
        sql(paste("distinct", x))
      },
      var = sql_aggregate_compat("variance", "var"),
      variance = sql_aggregate("variance"),
      variance_samp = sql_aggregate("variance_samp"),
      variance_pop = sql_aggregate("variance_pop"),
      var_samp = sql_aggregate("var_samp"),
      var_pop = sql_aggregate("var_pop"),
      paste = function(..., sep = " ", collapse = NULL) {
        if (is.null(collapse)) {
          stop("To use paste() as an aggregate function, set the collapse argument",
               call. = FALSE)
        } else {
          if (length(list(...)) > 1) {
            sql_expr(group_concat(concat_ws(!!sep, !!!list(...)), !!collapse))
          } else {
            sql_expr(group_concat(!!!list(...), !!collapse))
          }
        }
      },
      paste0 = function(..., collapse = NULL) {
        if (is.null(collapse)) {
          stop("To use paste0() as an aggregate function, set the collapse argument",
               call. = FALSE)
        } else {
          if (length(list(...)) > 1) {
            sql_expr(group_concat(concat(!!!list(...)), !!collapse))
          } else {
            sql_expr(group_concat(!!!list(...), !!collapse))
          }
        }
      },
      str_c = function(..., sep = "", collapse = NULL) {
        if (is.null(collapse)) {
          stop("To use str_c() as an aggregate function, set the collapse argument",
               call. = FALSE)
        } else {
          if (length(list(...)) > 1) {
            sql_expr(group_concat(concat_ws(!!sep, !!!list(...)), !!collapse))
          } else {
            sql_expr(group_concat(!!!list(...), !!collapse))
          }
        }
      },
      str_collapse = function(x, collapse) {
        warning("str_collapse() is deprecated. Use str_flatten() instead.",
                call. = FALSE)
        sql_expr(group_concat(!!x, !!collapse))
      },
      str_flatten = function(x, collapse) {
        sql_expr(group_concat(!!x, !!collapse))
      }
    ),
    sql_translator(
      .parent = base_win,
      appx_median = win_absent("appx_median"),
      avg = win_aggregate("avg"),
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
      paste = function(..., sep = " ", collapse = NULL) {
        if (is.null(collapse)) {
          sql_expr(concat_ws(!!sep, !!!list(...)))
        } else {
          stop("paste() with collapse argument is not supported in window functions",
               call. = FALSE)
        }
      },
      paste0 = function(...,  collapse = NULL) {
        if (is.null(collapse)) {
          sql_expr(concat(!!!list(...)))
        } else {
          stop("paste0() with collapse argument is not supported in window functions",
               call. = FALSE)
        }
      },
      str_c = function(..., sep = "", collapse = NULL) {
        if (is.null(collapse)) {
          sql_expr(concat_ws(!!sep, !!!list(...)))
        } else {
          stop("str_c() with collapse argument is not supported in window functions",
               call. = FALSE)
        }
      },
      str_collapse = win_absent("str_collapse"),
      str_flatten = win_absent("str_flatten"),
      sd = win_absent("sd"),
      stddev = win_absent("stddev"),
      stddev_samp = win_absent("stddev_samp"),
      stddev_pop = win_absent("stddev_pop"),
      unique = function(x) {
        sql(paste("distinct", x))
      },
      var = win_absent("var"),
      variance = win_absent("variance"),
      variance_samp = win_absent("variance_samp"),
      variance_pop = win_absent("variance_pop"),
      var_samp = win_absent("var_samp"),
      var_pop = win_absent("var_pop")
    )
  )
}

#' @importFrom dbplyr sql_aggregate
sql_aggregate_compat <- function(f, ...) {
  # This function allows the SQL translations to support dbplyr 1.3.0
  # and earlier while using the optional f_r argument to sql_aggregate()
  if (length(args(sql_aggregate)) < 2) {
    sql_aggregate(f)
  } else {
    sql_aggregate(f, ...)
  }
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

globalVariables(c("concat", "concat_ws", "group_concat", "trim", "ltrim", "rtrim", "length"))
