% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/src_impala.R
\name{dbDisconnect,src_impala-method}
\alias{dbDisconnect,src_impala-method}
\title{Close the connection to Impala}
\usage{
\S4method{dbDisconnect}{src_impala}(conn, ...)
}
\arguments{
\item{conn}{object with class class \code{src_impala}}

\item{...}{other arguments passed on to methods}
}
\value{
Returns \code{TRUE}, invisibly
}
\description{
Closes (disconnects) the connection to Impala.
}
\examples{
\dontrun{
dbDisconnect(impala)}
}
