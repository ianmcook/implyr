# Copyright 2024 Cloudera Inc.
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
  sql_out <- sql_render(qry, con = con, ...)
  # hack to remove table name alias from complex (nested) column queries
  sql_out <- gsub("(`.+?`\\, `.+?`\\.`.+?`) `q[0-9]+`", "\\1", sql_out)
  sql_out
}

has_order_by_in_subquery <- function(x) {
  if (inherits(x, "dbplyr_table_ident")) {
    return(FALSE)
  }
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
