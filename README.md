
<!-- README.md is generated from README.Rmd. Please edit that file -->

# implyr <img src="man/figures/logo.png" align="right" height="139" />

## Overview

**implyr** is a SQL backend to
[dplyr](https://cran.r-project.org/package=dplyr) for [Apache
Impala](https://impala.apache.org), the massively parallel processing
query engine. Impala enables low-latency SQL queries on large datasets
stored in HDFS, Apache HBase, Apache Kudu, Amazon S3, Microsoft ADLS,
and Dell EMC Isilon.

implyr is designed to work with any
[DBI](https://cran.r-project.org/package=DBI)-compatible interface to
Impala. implyr does not provide the underlying connectivity to Impala,
nor does it require that you use one particular R package for
connectivity to Impala. Currently, two packages that can provide this
connectivity are [odbc](https://cran.r-project.org/package=odbc) and
[RJDBC](https://cran.r-project.org/package=RJDBC). Future packages may
provide other options for connectivity.

## Installation

You can install the latest release of implyr from CRAN:

``` r
install.packages("implyr")
```

Or you can install the current development version from GitHub:

``` r
devtools::install_github("ianmcook/implyr")
```

You must also install a package and driver to provide connectivity to
Impala.

#### ODBC Connectivity

[odbc](https://cran.r-project.org/package=odbc) is currently the
preferred R package for connecting to Impala. It provides superior
performance and compatibility.

1.  Ensure that the system where your R code will run supports ODBC.
    ODBC support is built into Windows but requires
    [unixODBC](http://www.unixodbc.org) or [iODBC](http://www.iodbc.org)
    on Linux and macOS.

2.  Install the odbc package from CRAN:
    
    ``` r
    install.packages("odbc")
    ```

3.  Download and install the latest version of the [Impala ODBC driver
    from
    Cloudera](https://www.cloudera.com/downloads/connectors/impala/odbc.html).

4.  Complete the installation and configuration steps described in the
    [odbc package
    README](https://cran.r-project.org/package=odbc/readme/README.html#installation)
    and the [Impala ODBC driver installation
    guide](https://www.cloudera.com/content/www/en-us/documentation/other/connectors/impala-odbc/latest/Cloudera-ODBC-Driver-for-Impala-Install-Guide.pdf).

#### JDBC Connectivity

Package [RJDBC](https://cran.r-project.org/package=RJDBC) can provide
access to Impala through JDBC.

1.  Ensure that the system where your R code will run has a Java Runtime
    Environment (JRE) installed.

2.  Install the RJDBC package from CRAN:
    
    ``` r
    install.packages("RJDBC")
    ```

3.  Download and install the latest version of the [Impala JDBC driver
    from
    Cloudera](https://www.cloudera.com/downloads/connectors/impala/jdbc.html).

4.  Complete the installation and configuration steps described in the
    [Impala JDBC driver installation
    guide](https://www.cloudera.com/content/www/en-us/documentation/other/connectors/impala-jdbc/latest/Cloudera-JDBC-Driver-for-Impala-Install-Guide.pdf).

## Connecting to Impala

First, load the implyr package:

``` r
library(implyr)
```

The next step depends on the method you will use to connect to Impala.

#### ODBC Connectivity

Load the odbc package:

``` r
library(odbc)
```

Create an ODBC driver object:

``` r
drv <- odbc::odbc()
```

Call `src_impala()` to connect to Impala and create a dplyr data source.
In the call to `src_impala()`, specify the arguments required by
`odbc::dbConnect()`. These arguments can consist of individual ODBC
keywords (`driver`, `host`, `port`, `database`, `uid`, `pwd`, and
others), an ODBC data source name (`dsn`), or an ODBC connection string
(`.connection_string`). For example:

``` r
impala <- src_impala(
    drv = drv,
    driver = "Cloudera ODBC Driver for Impala",
    host = "host",
    port = 21050,
    database = "default",
    uid = "username",
    pwd = "password"
  )
```

The returned object `impala` provides a remote dplyr data source to
Impala.

For more information about which arguments you can pass to
`src_impala()` when using ODBC connectivity, see
`?"dbConnect,OdbcDriver-method"` and the
[Authentication](#authentication) section below.

#### JDBC Connectivity

Load the RJDBC package:

``` r
library(RJDBC)
```

Initialize the Java Virtual Machine (JVM) by calling `.jinit()` and
passing a vector containing the paths to all the Impala JDBC driver JAR
files as the `classpath` argument:

``` r
impala_classpath <- list.files(path = "/path/to/jdbc/driver", pattern = "\\.jar$", full.names = TRUE)
.jinit(classpath = impala_classpath)
```

If an error occurs, you may need to first set the `JAVA_HOME`
environment variable:

``` r
Sys.setenv(JAVA_HOME = "/path/to/java/home/")
```

Create a JDBC driver object:

``` r
drv <- JDBC(
  driverClass = "com.cloudera.impala.jdbc41.Driver",
  classPath = impala_classpath,
  identifier.quote = "`"
)
```

If you are using the JDBC version 4.0 driver, specify
`com.cloudera.impala.jdbc4.Driver`. If you are using the JDBC version
4.1 driver, specify `com.cloudera.impala.jdbc41.Driver`.

Call `src_impala()` to connect to Impala and create a dplyr data source.
In the call to `src_impala()`, specify a JDBC connection string as the
first argument. Optionally, specify a username as the second argument
and a password as the third argument. For example:

``` r
impala <- src_impala(drv, "jdbc:impala://host:21050", "username", "password")
```

Or include the username and password (or other authentication
properties) in the connection string:

``` r
impala <- src_impala(drv, "jdbc:impala://host:21050;UID=username;PWD=password")
```

The returned object `impala` provides a remote dplyr data source to
Impala.

See the [Authentication](#authentication) section below for information
about how to construct the JDBC connection string when using different
authentication methods.

Do not attempt to connect to Impala using more than one method in one R
session.

#### Authentication

The Impala ODBC and JDBC drivers support multiple authentication
methods, including no authentication, username, username and password,
and Kerberos. To use Kerberos, specify properties including `AuthMech`,
`KrbRealm`, `KrbHostFQDN`, and `KrbServiceName`. Consult your system
administrator and the [Impala ODBC driver installation
guide](https://www.cloudera.com/content/www/en-us/documentation/other/connectors/impala-odbc/latest/Cloudera-ODBC-Driver-for-Impala-Install-Guide.pdf)
or [Impala JDBC driver installation
guide](https://www.cloudera.com/content/www/en-us/documentation/other/connectors/impala-jdbc/latest/Cloudera-JDBC-Driver-for-Impala-Install-Guide.pdf).

## Using dplyr

Now you can use dplyr verbs against tables in Impala.

To see what tables are in the current database in Impala, issue the
command:

``` r
src_tbls(impala)
```

For this example, start by creating a lazy `tbl` named `flights_tbl`
representing the data in the Impala table named `flights`:

``` r
flights_tbl <- tbl(impala, "flights")
```

To specify the database that contains the table, use the function
`in_schema()`. For example, if the Impala table named `flights` were in
a database named `nycflights13`, then you would use the command:

``` r
flights_tbl <- tbl(impala, in_schema("nycflights13", "flights"))
```

The examples here assume that data has already been loaded into the
Impala table named `flights`. See the [Loading Local Data into
Impala](#loading-local-data-into-impala) section below for information
about ways to load data from R into Impala.

<!-- if you change this example, also change the corresponding test in test-readme.R -->

``` r
delay <- flights_tbl %>% 
  select(tailnum, distance, arr_delay) %>%
  group_by(tailnum) %>%
  summarise(count = n(), dist = mean(distance), delay = mean(arr_delay)) %>%
  filter(count > 20L, dist < 2000L, !is.na(delay)) %>%
  arrange(delay, dist, count) %>%
  collect()
```

implyr supports the dplyr verbs `filter()`, `arrange()`, `select()`,
`rename()`, `distinct()`, `mutate()`, `transmute()`, and `summarise()`.
It supports grouped operations with the `group_by()` function.

When using implyr, you must specify table names and column names using
lowercase characters. To ensure that results are in sorted order, you
must apply `arrange()` last, after all other dplyr verbs.

Impala does not perform implicit casting; for example, it does not
automatically convert numbers to strings when they are used in a string
context. Impala requires that you explicitly cast columns to the
required types. implyr provides familiar R-style type conversion
functions to enable casting to all the scalar [Impala data
types](https://www.cloudera.com/documentation/enterprise/latest/topics/impala_datatypes.html).
For example, `as.character()` casts a column or column expression to the
Impala `STRING` type.

<!-- if you change this example, also change the corresponding test in test-readme.R -->

``` r
flights_tbl %>% 
  transmute(flight_code = paste0(carrier, as.character(flight))) %>% 
  distinct(flight_code)
```

In addition, you should specify integer values as R `integer` objects
instead of `numeric` objects; for example, `1L` or `as.integer(1)`
instead of `1`.

See [Introduction to
dplyr](https://cran.r-project.org/package=dplyr/vignettes/dplyr.html)
for more examples of dplyr grammar.

Like other SQL backends to dplyr, implyr delays work until a result
needs to be computed, then computes the result as a single query
operation.

  - Use `collect()` to execute the query and return the result to R as a
    data frame `tbl`.
  - Use `as.data.frame()` to execute the query and return the result to
    R as an ordinary data frame.
  - Use `compute(temporary = FALSE)` to execute the query and store the
    result in an Impala table. Impala does not support temporary tables,
    so `temporary = FALSE` is required.
  - Use `collapse()` to generate the query for later execution.

If you print or store a result without using one of these functions,
then implyr returns a lazy `tbl`. Only use `collect()` or
`as.data.frame()` when the result will be small enough to fit in memory
in your R session.

See the [Introduction to
d**b**plyr](https://cran.r-project.org/package=dbplyr/vignettes/dbplyr.html)
for more information.

implyr supports window functions, which enable computation of ranks,
offsets, and cumulative aggregates. See [Window
functions](https://cran.r-project.org/package=dplyr/vignettes/window-functions.html)
for more information.

<!-- if you change this example, also change the corresponding test in test-readme.R -->

``` r
worst_delay_each_day <- flights_tbl %>%
  group_by(year, month, day) %>%
  filter(arr_delay == max(arr_delay)) %>%
  arrange(year, month, day) %>%
  collect()
```

implyr supports most [two-table
verbs](https://cran.r-project.org/package=dplyr/vignettes/two-table.html),
which enable joins and set operations.

<!-- if you change this example, also change the corresponding test in test-readme.R -->

``` r
airlines_tbl <- tbl(impala, "airlines")
inner_join(flights_tbl, airlines_tbl, by = "carrier")
```

implyr supports efficient filtering joins.

<!-- if you change this example, also change the corresponding test in test-readme.R -->

``` r
airlines_tbl <- tbl(impala, "airlines")
southwest_airlines <- airlines_tbl %>% filter(name == "Southwest Airlines Co.")
southwest_flights <- semi_join(flights_tbl, southwest_airlines, by = "carrier")
```

You can also use dplyr join functions to bring together values from
`ARRAY` and `MAP` columns with scalar values from the same rows. See
[Impala Complex
Types](https://www.cloudera.com/documentation/enterprise/latest/topics/impala_complex_types.html)
for more details about `ARRAY` and `MAP` columns.

Read the [Warnings and Current
Limitations](#warnings-and-current-limitations) section below to
understand the ways that working with Impala as a remote dplyr data
source is different from working with local data or other remote dplyr
data sources.

<!-- if you add more examples here, also add corresponding tests in test-readme.R -->

## Using SQL

In addition to using dplyr grammar, you can also issue SQL queries to
Impala.

To execute a statement that returns no result set, use the `dbExecute()`
function:

<!-- if you change this example, also change the corresponding test in test-readme.R -->

``` r
dbExecute(impala, "REFRESH flights")
```

To execute a query and return the result to R as a data frame, use the
`dbGetQuery()` function.

<!-- if you change this example, also change the corresponding test in test-readme.R -->

``` r
flights_by_carrier_df <- dbGetQuery(
  impala,
  "SELECT carrier, COUNT(*) FROM flights GROUP BY carrier"
)
```

Only use `dbGetQuery` when the query result will be small enough to fit
in memory in your R session.

You can also execute SQL and return the result as a lazy `tbl`:

<!-- if you change this example, also change the corresponding test in test-readme.R -->

``` r
flights_tbl <- tbl(impala, sql("SELECT * FROM flights"))
```

<!-- if you add more examples here, also add corresponding tests in test-readme.R -->

## Disconnecting

When you are finished, close the connection to Impala:

``` r
dbDisconnect(impala)
```

## Loading Local Data into Impala

The examples above assume that data has already been loaded into Impala.
If you wish to run the examples above, you will need to load data from
the package
[nycflights13](https://cran.r-project.org/package=nycflights13) into
Impala.

implyr does not provide tools for loading local data into Impala tables.
This is because Impala can query data stored in several different
filesystems and storage systems (HDFS, Apache HBase, Apache Kudu, Amazon
S3, Microsoft ADLS, and Dell EMC Isilon) and Impala does not include
built-in capability for loading local data into these systems.

Some other dplyr backends implement the function `copy_to`, which copies
a local data frame to a remote source. `implyr` implements `copy_to`,
but it currently only supports very small data frames. It uses the SQL
`INSERT ... VALUES()` technique, which is not suitable for loading large
amounts of data.

HDFS is the most common system for storing data in Impala tables. There
are two methods described below for uploading data from R into HDFS.

To load the data frame `nycflights13::flights` into HDFS, first install
and load the nycflights13 package:

``` r
install.packages("nycflights13")
library(nycflights13)
```

Then issue an SQL statement to create a table `flights` in Impala with a
schema that matches the data frame `flights`:

``` r
dbExecute(impala, "CREATE TABLE flights (
    year SMALLINT,
    month TINYINT,
    day TINYINT,
    dep_time SMALLINT,
    sched_dep_time SMALLINT,
    dep_delay SMALLINT,
    arr_time SMALLINT,
    sched_arr_time SMALLINT,
    arr_delay SMALLINT,
    carrier STRING,
    flight SMALLINT,
    tailnum STRING,
    origin STRING,
    dest STRING,
    air_time SMALLINT,
    distance SMALLINT,
    hour TINYINT,
    minute TINYINT,
    time_hour TIMESTAMP)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
  LOCATION '/user/hive/warehouse/flights'")
```

Next, write the data frame `flights` to a local file:

``` r
write.table(flights, file = "flights", quote = FALSE, sep = "\t", na = "\\N", row.names = FALSE, col.names = FALSE)
```

If the Hadoop File System shell is installed on the system where you are
running R, then you can issue an `hdfs dfs -put` command to load this
local file to HDFS. You can issue the command from R using the
`system()` function:

``` r
system("hdfs dfs -put flights /user/hive/warehouse/flights/000000_0")
```

Another option is to use the R package
[rwebhdfs](https://github.com/saurfang/rwebhdfs) to load the local file
into HDFS using the WebHDFS REST API:

``` r
devtools::install_github("saurfang/rwebhdfs")
library(rwebhdfs)
hdfs <- webhdfs("host", 50070, "username")
write_file(hdfs, "/user/hive/warehouse/flights/000000_0", "flights")
```

After loading the data, issue the Impala command `INVALIDATE METADATA`
to refresh Impala’s metadata cache:

``` r
dbExecute(impala, "INVALIDATE METADATA")
```

## Warnings and Current Limitations

#### Using implyr with RJDBC

The RJDBC package is not fully DBI-compatible. implyr works around these
incompatibilities as best it can.

RJDBC has a crude type handling system: columns and column expressions
with numeric types are returned to R as `numeric` columns, and all other
types are returned as `character` columns. This has undesirable effects;
for example, Boolean types are returned as `character` columns with
values `"0"` for `FALSE` and or `"1"` for `TRUE`.

When using the function `copy_to` with RJDBC, data types may be modified
in unexpected ways. For example, inserted `character` values may be
right-padded with whitespace.

If possible, connect to Impala using the odbc package instead of the
RJDBC package.

#### Row Order

Impala’s data storage and processing does not preserve row order. Impala
uses parallel processing and stores data in multiple files, so the the
notion of data being stored in sorted order is impractical. This has
several important implications for the use of implyr:

  - Rows are not necessarily returned in the same order that they were
    in when added to Impala. To return rows in a specific order, you
    must use `arrange()`.
  - If row ordering is applied in an intermediate phase of query
    processing, Impala may not return the final result in sorted order.
    To ensure that results are in sorted order, apply `arrange()` last,
    after all other dplyr verbs. implyr will issue a warning if you
    apply `arrange()` in an earlier step.
  - When using `compute()` to store results in an Impala table, Impala
    may not preserve row order. implyr will issue a warning if you use
    `arrange()` before `compute()`.

See the [Impala ORDER BY
documentation](https://www.cloudera.com/documentation/enterprise/latest/topics/impala_order_by.html)
for more information.

#### Temporary Tables

Impala does not support temporary tables. When using `compute()` to
store results in an Impala table, you must set `temporary = FALSE`.
implyr will throw an error if you use `compute()` but do not set
`temporary = FALSE`.

#### Missing Values

SQL engines including Impala treat missing values differently than R
does. To avoid unexpected results, handle missing values before applying
other operations on column values.

#### Table and Column Names

Impala requires table names and column names to be all lowercase.
Currently, implyr does not convert table names and column names to
lowercase; you must specify them using all lowercase characters. For
information about other limitations on table names and column names, see
[Overview of Impala
Identifiers](https://www.cloudera.com/documentation/enterprise/latest/topics/impala_identifiers.html).

#### dplyr Support

implyr does not support all dplyr verbs and functions. Some verbs
including `slice()`, `sample_n()`, and `sample_frac()` are not
supported. Some functions including `intersect()` and `setdiff()` are
not supported.

If you apply an R function to a lazy `tbl` and the function is not
implemented as a remote method, then implyr will compute the result of
the prior steps and return that result to R as a `tbl` or data frame. It
will then compute the function and any later steps locally in R. An
example of this is the function `lm`. Only use this technique when the
intermediate result will be small enough to fit in memory in your R
session.

The `median()` function returns a value that is approximately (not
necessarily exactly) the median. See [APPX\_MEDIAN
Function](https://www.cloudera.com/documentation/enterprise/latest/topics/impala_appx_median.html).

implyr supports some Impala functions that are not specified by R or by
dplyr. See [Impala Built-In
Functions](https://www.cloudera.com/documentation/enterprise/latest/topics/impala_functions.html)
for more information.
