
is_dbplyr_table_identifier <- function(x) {
  inherits(x, "dbplyr_table_ident") || # dplyr 2.4.0
    inherits(x, "dplyr_table_path") # dplyr >= 2.5.0
}

as_dbplyr_table_identifier <- function(x) {
  x <- ident_q(x)

  if (packageVersion("dplyr") >= "2.4.0.9000") {
    # now exported, but this avoids an R CMD check WARNING
    utils::getFromNamespace("as_table_path", "dbplyr")(x)
  } else {
    utils::getFromNamespace("as_table_ident", "dbplyr")(x)
  }
}
