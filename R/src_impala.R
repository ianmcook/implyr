# Copyright 2017 Cloudera Inc.
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

# register virtual classes

#' @importFrom methods setOldClass
setOldClass("src_impala")

#' @importFrom methods setOldClass
setOldClass("tbl_impala")

# environment for global variables
pkg_env <- new.env()
pkg_env$order_by_in_subquery <- FALSE
pkg_env$order_by_in_query <- FALSE

#' Connect to Impala and create a remote dplyr data source
#'
#' @description
#' \code{src_impala} creates a SQL backend to dplyr for
#' \href{https://impala.incubator.apache.org/}{Apache Impala (incubating)},
#' the massively parallel processing query engine for Apache Hadoop.
#'
#' \code{src_impala} can work with any DBI-compatible interface that provides
#' connectivity to Impala. Currently, two packages that can provide this
#' connectivity are odbc and RJDBC.
#'
#' @param drv an object that inherits from \code{\link[DBI]{DBIDriver-class}}.
#'   For example, an object returned by \code{\link[odbc]{odbc}} or
#'   \code{\link[RJDBC]{JDBC}}
#' @param ... arguments passed to the underlying Impala database connection
#'   method \code{\link[DBI]{dbConnect}}. See
#'   \code{\link[odbc]{dbConnect,OdbcDriver-method}} or
#'   \code{\link[RJDBC]{dbConnect,JDBCDriver-method}}
#' @param auto_disconnect Should the connection to Impala be automatically
#'   closed when the object returned by this function is deleted? Pass \code{NA}
#'   to auto-disconnect but print a message when this happens.
#' @return An object with class \code{src_impala}, \code{src_sql}, \code{src}
#' @examples
#' # Using ODBC connectivity:
#'
#' \dontrun{
#' library(odbc)
#' drv <- odbc::odbc()
#' impala <- src_impala(
#'   drv = drv,
#'   driver = "Cloudera ODBC Driver for Impala",
#'   host = "host",
#'   port = 21050,
#'   database = "default",
#'   uid = "username",
#'   pwd = "password"
#' )}
#'
#' # Using JDBC connectivity:
#'
#' \dontrun{
#' library(RJDBC)
#' Sys.setenv(JAVA_HOME = "/path/to/java/home/")
#' impala_classpath <- list.files(
#'   path = "/path/to/jdbc/driver",
#'   pattern = "\\.jar$",
#'   full.names = TRUE
#' )
#' .jinit(classpath = impala_classpath)
#' drv <- JDBC(
#'   driverClass = "com.cloudera.impala.jdbc41.Driver",
#'   classPath = impala_classpath,
#'   identifier.quote = "`"
#' )
#' impala <- src_impala(
#'   drv,
#'   "jdbc:impala://host:21050",
#'   "username",
#'   "password"
#' )}
#' @seealso
#' \href{https://www.cloudera.com/downloads/connectors/impala/odbc.html}{Impala
#' ODBC driver},
#' \href{https://www.cloudera.com/downloads/connectors/impala/jdbc.html}{Impala
#' JDBC driver}
#' @export
#' @importFrom DBI dbConnect
#' @importFrom DBI dbExecute
#' @importFrom DBI dbGetInfo
#' @importFrom DBI dbSendQuery
#' @importFrom dbplyr src_sql
#' @importFrom methods callNextMethod
#' @importFrom methods isClass
#' @importFrom methods removeClass
#' @importFrom methods getClass
#' @importFrom methods setClass
#' @importFrom methods setMethod
#' @importFrom rlang is_false
#' @importFrom rlang is_true
#' @importFrom utils getFromNamespace
src_impala <- function(drv, ..., auto_disconnect = TRUE) {
  if (!requireNamespace("assertthat", quietly = TRUE)) {
    stop("assertthat is required to use src_impala", call. = FALSE)
  }
  if (!requireNamespace("dplyr", quietly = TRUE)) {
    stop("dplyr is required to use src_impala", call. = FALSE)
  }
  if (!requireNamespace("dbplyr", quietly = TRUE)) {
    stop("dbplyr is required to use src_impala", call. = FALSE)
  }
  if (!requireNamespace("DBI", quietly = TRUE)) {
    stop("DBI is required to use src_impala", call. = FALSE)
  }
  if (inherits(drv, "src_impala")) {
    con <- drv
    return(con)
  }
  if (!inherits(drv, "DBIDriver")) {
    stop("drv must be a DBI-compatible driver object or an existing src_impala object",
         call. = FALSE)
  }

  con <- dbConnect(drv, ...)

  if (is_false(auto_disconnect)) {
    disco <- NULL
  } else {
    disco <- db_disconnector(con, quiet = is_true(auto_disconnect))
  }

  r <- dbGetQuery(con,
                  "SELECT version() AS version, current_database() AS dbname;")
  if (inherits(con, "JDBCConnection")) {
    l <- getNamedArgs_JDBCDriver(...)
    info <- list(
      user = l$user,
      url = sub(".+?://", "", sub(
        paste0("(:\\d*/)", r$dbname), "\\1", l$url
      )),
      version = r$version,
      dbname = r$dbname
    )
  } else if (inherits(con, "OdbcConnection")) {
    l <- getNamedArgs_OdbcDriver(...)
    if (!is.null(l$.connection_string)) {
      if (grepl("Host=(.+?);", l$.connection_string, ignore.case = TRUE)) {
        l$host <-
          sub(".*Host=(.+?);.*",
              "\\1",
              l$.connection_string,
              ignore.case = TRUE)
      }
      if (grepl("Port=(.+?);", l$.connection_string, ignore.case = TRUE)) {
        l$port <-
          sub(".*Port=(.+?);.*",
              "\\1",
              l$.connection_string,
              ignore.case = TRUE)
      }
      if (grepl("UID=(.+?);", l$.connection_string, ignore.case = TRUE)) {
        l$uid <-
          sub(".*UID=(.+?);.*",
              "\\1",
              l$.connection_string,
              ignore.case = TRUE)
      }
    }
    info <- list(
      dsn = l$dsn,
      user = l$uid,
      host = l$host,
      port = l$port,
      version = r$version,
      dbname = r$dbname
    )
  } else {
    info <- dbGetInfo(con)
  }
  info$package <-
    attr(attr(getClass(class(con)[1]), "className"), "package")

  if (isClass("impala_connection", where = .GlobalEnv)) {
    removeClass("impala_connection", where = .GlobalEnv)
  }
  setClass("impala_connection",
           contains = class(con),
           where = .GlobalEnv)

  setMethod("dbSendQuery", c("impala_connection", "character"), function(conn, statement, ...) {
    result <- methods::callNextMethod(conn, statement, ...)
    if (isTRUE(pkg_env$order_by_in_subquery)) {
      warning(
        "Results may not be in sorted order! Move arrange() after all other verbs for results in sorted order."
      )
    }
    pkg_env$order_by_in_subquery <- FALSE
    pkg_env$order_by_in_query <- FALSE
    result
  }, where = .GlobalEnv)

  setMethod("dbExecute", c("impala_connection", "character"), function(conn, statement, ...) {
    if (inherits(conn, "JDBCConnection")) {
      result <-
        utils::getFromNamespace("dbSendUpdate", "RJDBC")(conn, statement)
    } else {
      result <- methods::callNextMethod(conn, statement, ...)
    }
    if (isTRUE(pkg_env$order_by_in_query)) {
      warning("Results may not be in sorted order! Impala cannot store data in sorted order.")
    }
    pkg_env$order_by_in_subquery <- FALSE
    pkg_env$order_by_in_query <- FALSE
    result
  }, where = .GlobalEnv)

  con <- structure(con, class = c("impala_connection", class(con)))
  attributes(con)$info <- info

  src_sql("impala",
          con = con,
          disco = disco,
          info = info)
}

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

#' Create a lazy \code{tbl} from an Impala table
#'
#' @name tbl
#' @param src an object with class with class \code{src_impala}
#' @param from a table name or identifier
#' @param ... not used
#' @return An object with class \code{tbl_impala}, \code{tbl_sql},
#'   \code{tbl_lazy}, \code{tbl}
#' @examples
#' \dontrun{
#' flights_tbl <- tbl(impala, "flights")
#'
#' flights_tbl <- tbl(impala, in_schema("nycflights13", "flights"))}
#' @seealso \code{\link{in_schema}}
#' @export
#' @importFrom dbplyr tbl_sql
#' @importFrom dplyr tbl
tbl.src_impala <- function(src, from, ...) {
  tbl_sql("impala", src = src, from = from, ...)
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
#' @importFrom dbplyr db_sql_render
#' @importFrom dbplyr sql_build
#' @importFrom dbplyr sql_render
db_sql_render.impala_connection <- function(con, sql, ...) {
  qry <- sql_build(sql, con = con, ...)
  if (has_order_by_in_subquery(qry)) {
    pkg_env$order_by_in_subquery <- TRUE
  }
  if (has_order_by(qry)) {
    pkg_env$order_by_in_query <- TRUE
  }
  sql_render(qry, con = con, ...)
}

has_order_by_in_subquery <- function(x) {
  if (!inherits(x$from, "select_query")) {
    return(FALSE)
  }
  if (length(x$from$order_by) > 0) {
    return(TRUE)
  }
  has_order_by_in_subquery(x$from)
}

has_order_by <- function(x) {
  if (!inherits(x, "select_query")) {
    return(FALSE)
  }
  if (length(x$order_by) > 0) {
    return(TRUE)
  }
  has_order_by(x$from)
}

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

#' Copy a (very small) local data frame to Impala
#'
#' @name copy_to
#' @description
#' \code{copy_to} inserts the contents of a local data frame into a new Impala
#' table. \code{copy_to} currently only supports very small data frames (1000 or
#' fewer row/column positions). It uses the SQL \code{INSERT ... VALUES()}
#' technique, which is not suitable for loading large amounts of data.
#'
#' This package does not provide tools for loading larger amounts of local data
#' into Impala tables. This is because Impala can query data stored in several
#' different filesystems and storage systems (HDFS, Apache Kudu, Apache HBase,
#' and Amazon S3) and Impala does not include built-in capability for loading
#' local data into these systems.
#'
#' @param dest an object with class with class \code{src_impala}
#' @param df a (very small) local data frame
#' @param name name for the new Impala table
#' @param overwrite whether to overwrite existing table data (currently ignored)
#' @param types a character vector giving variable types to use for the columns
#' @param temporary must be set to \code{FALSE}
#' @param unique_indexes not used
#' @param indexes not used
#' @param analyze whether to run \code{COMPUTE STATS} after adding data to the
#'   new table
#' @param external whether the new table will be externally managed
#' @param force whether to silently continue if the table already exists
#' @param field_terminator the deliminter to use between fields in text file
#'   data. Defaults to the ASCII control-A (hex 01) character
#' @param line_terminator the line terminator. Defaults to \code{"\n"}
#' @param file_format the storage format to use. Options are \code{"TEXTFILE"}
#'   (default) and \code{"PARQUET"}
#' @param ... other arguments passed on to methods
#' @return An object with class \code{tbl_impala}, \code{tbl_sql},
#'   \code{tbl_lazy}, \code{tbl}
#' @examples
#' library(nycflights13)
#' dim(airlines) # airlines data frame is very small
#' # [1] 16  2
#'
#' \dontrun{
#' copy_to(impala, airlines, temporary = FALSE)}
#' @note Impala does not support temporary tables. When using \code{copy_to()}
#'   to insert local data into an Impala table, you must set \code{temporary =
#'   FALSE}.
#' @export
#' @importFrom assertthat assert_that
#' @importFrom assertthat is.string
#' @importFrom assertthat is.flag
#' @importFrom dplyr copy_to
copy_to.src_impala <-
  function(dest,
           df,
           name = deparse(substitute(df)),
           overwrite = FALSE,
           types = NULL,
           temporary = TRUE,
           unique_indexes = NULL,
           indexes = NULL,
           analyze = TRUE,
           external = FALSE,
           force = FALSE,
           field_terminator = NULL,
           line_terminator = NULL,
           file_format = NULL,
           ...) {
    # don't try to insert large data frames with INSERT ... VALUES()
    if (prod(dim(df)) > 1e3L) {
      # TBD: consider whether to make this limit configurable, possibly using
      #  options with the pkgconfig package
      stop(
        "Data frame ",
        name,
        " is too large. copy_to currently only supports very small data frames.",
        call. = FALSE
      )
    }

    # TBD: add params to control location and other CREATE TABLE options

    assert_that(
      is.data.frame(df),
      is.string(name),
      is.flag(overwrite),
      is.flag(temporary),
      is.flag(analyze),
      is_string_or_null(file_format),
      is_nchar_one_string_or_null(field_terminator),
      is_nchar_one_string_or_null(line_terminator)
    )
    if (temporary) {
      stop(
        "Impala does not support temporary tables. Set temporary = FALSE in copy_to().",
        call. = FALSE
      )
    }
    class(df) <- "data.frame"
    con <- con_acquire(dest)
    tryCatch({
      types <- types %||% db_data_type(con, df)
      names(types) <-
        names(df) # TBD: convert illegal names to legal names?
      tryCatch({
        db_create_table(
          con = con,
          table = name,
          types = types,
          temporary = FALSE,
          external = external,
          force = force,
          field_terminator = field_terminator,
          line_terminator = field_terminator,
          file_format = file_format,
          ...
        )
        db_insert_into(con, name, df, overwrite)
        if (analyze) {
          db_analyze(con, name)
        }
      }, error = function(err) {
        stop(err)
      })
    }, finally = {
      con_release(dest, con)
    })
    invisible(tbl(dest, name))
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

con_acquire <- function (src) {
  con <- src$con
  if (is.null(con)) {
    stop("No connection found", call. = FALSE)
  }
  con
}
con_release <- function(src, con) {
  # do nothing
}

#' Send SQL query to Impala and retrieve results
#'
#' @description Returns the result of an Impala SQL query as a data frame.
#'
#' @param conn object with class class \code{src_impala}
#' @param statement a character string containing SQL
#' @param ... other arguments passed on to methods
#' @return A \code{data.frame} with as many rows as records were fetched and as
#'   many columns as fields in the result set, even if the result is a single
#'   value or has one or zero rows
#' @examples
#' \dontrun{
#' flights_by_carrier_df <- dbGetQuery(
#'   impala,
#'   "SELECT carrier, COUNT(*) FROM flights GROUP BY carrier"
#' )}
#' @note This method is for \code{SELECT} queries only. Use
#'   \code{\link[=dbExecute,src_impala,character-method]{dbExecute()}} for data
#'   definition or data manipulation statements.
#' @export
#' @importFrom DBI dbGetQuery
#' @importFrom methods setMethod
setMethod("dbGetQuery", c("src_impala", "character"), function(conn, statement, ...) {
  dbGetQuery(con_acquire(conn), statement, ...)
})

#' Execute an Impala statement that returns no result
#'
#' @description Executes an Impala statement that returns no result.
#'
#' @param conn object with class class \code{src_impala}
#' @param statement a character string containing SQL
#' @param ... other arguments passed on to methods
#' @return Depending on the package used to connect to Impala, either a scalar
#'   numeric that specifies the number of rows affected by the statement, or
#'   \code{NULL}
#' @examples
#' \dontrun{
#' dbExecute(impala, "INVALIDATE METADATA")}
#' @note This method is for statements that return no result, such as data
#'   definition or data manipulation statements. Use
#'   \code{\link[=dbGetQuery,src_impala,character-method]{dbGetQuery()}} for
#'   \code{SELECT} queries.
#' @export
#' @importFrom DBI dbExecute
#' @importFrom methods setMethod
setMethod("dbExecute", c("src_impala", "character"), function(conn, statement, ...) {
  dbExecute(con_acquire(conn), statement, ...)
})

#' Close the connection to Impala
#'
#' @description Closes (disconnects) the connection to Impala.
#'
#' @param conn object with class class \code{src_impala}
#' @param ... other arguments passed on to methods
#' @return Returns \code{TRUE}, invisibly
#' @examples
#' \dontrun{
#' dbDisconnect(impala)}
#' @export
#' @importFrom DBI dbDisconnect
#' @importFrom methods setMethod
setMethod("dbDisconnect", "src_impala", function(conn, ...) {
  dbDisconnect(con_acquire(conn), ...)
})

# Escape quotes with a backslash instead of doubling
sql_quote <- function(x, quote) {
  y <- gsub(quote, paste0("\\", quote), x, fixed = TRUE)
  y <- paste0(quote, y, quote)
  y[is.na(x)] <- "NULL"
  names(y) <- names(x)
  y
}

# Gets the dots after a JDBCDriver as a named list, omitting password
getNamedArgs_JDBCDriver <-
  function(url,
           user = "",
           password = "",
           ...) {
    list(url = url, user = user, ...)
  }

# Gets the dots after an OdbcDriver as a named list, omitting pwd
getNamedArgs_OdbcDriver <-
  function(dsn = NULL,
           ...,
           timezone = "UTC",
           driver = NULL,
           server = NULL,
           database = NULL,
           uid = NULL,
           pwd = NULL,
           .connection_string = NULL) {
    list(
      dsn = dsn,
      ...,
      timezone = timezone,
      driver = driver,
      server = server,
      database = database,
      uid = uid,
      .connection_string = .connection_string
    )
  }

# Creates an environment that disconnects the database when it's GC'd
db_disconnector <- function(con, quiet = FALSE) {
  reg.finalizer(environment(), function(...) {
    if (!quiet) {
      message("Auto-disconnecting ", class(con)[[1]])
    }
    dbDisconnect(con)
  })
  environment()
}

`%||%` <- function(x, y) {
  if (is.null(x)) {
    y
  } else {
    x
  }
}

#' @export
#' @importFrom dbplyr in_schema
dbplyr::in_schema

#' @importFrom assertthat is.string
is_string_or_null <- function(x) {
  is.null(x) || is.string(x)
}

assertthat::on_failure(is_string_or_null) <- function(call, env) {
  paste0(deparse(call$x), " is not a string")
}

#' @importFrom assertthat is.string
is_nchar_one_string_or_null  <- function(x) {
  is.null(x) || (is.string(x) && nchar(x) == 1)
}

assertthat::on_failure(is_nchar_one_string_or_null) <-
  function(call, env) {
    paste0(deparse(call$x), " is not a string with one character")
  }
