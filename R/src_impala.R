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
setOldClass("src_impala")

# environment for global variables
pkg_env <- new.env()
pkg_env$order_by_in_subquery <- FALSE
pkg_env$order_by_in_query <- FALSE

#' Connect to Impala and create a remote dplyr data source
#'
#' @description \code{src_impala} creates a SQL backend to dplyr for
#' \href{https://impala.apache.org/}{Apache Impala}, the massively parallel
#' processing query engine for Apache Hadoop.
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
#' @importFrom methods getAllSuperClasses
#' @importFrom methods getClass
#' @importFrom methods isClass
#' @importFrom methods removeClass
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
      if (isTRUE(grepl("Port=(.+?);", l$.connection_string, ignore.case = TRUE))) {
        l$port <-
          sub(".*Port=(.+?);.*",
              "\\1",
              l$.connection_string,
              ignore.case = TRUE)
      }
      if (isTRUE(grepl("UID=(.+?);", l$.connection_string, ignore.case = TRUE))) {
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

  superclasses <- getAllSuperClasses(getClass(class(con)))
  for (superclass in superclasses) {
    superclass_package <- attr(getClass(superclass), "package")
    if (superclass_package != ".GlobalEnv") {
      info$package <-superclass_package
      break
    }
  }

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
        "Results may not be in sorted order! Move arrange() after all other verbs for results in sorted order.",
        call. = FALSE
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
      warning(
        "Results may not be in sorted order! Impala cannot store data in sorted order.",
        call. = FALSE
      )
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
