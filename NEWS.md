# implyr 0.5.0

* Suppressed an object not found warning (#51)
* Updated for compatibility with dbplyr 2.4.0 (#59, @liudvikasakelis)
* Required newer versions of Depends and Imports packages

# implyr 0.4.0

* Fixed bugs in table creation (#47, @karoliskascenas)
* Allow user to set `copy_to()` size limit with option `implyr.copy_to_size_limit` (#49, @karoliskascenas)
* Added more SQL translations
* Fixed a DBI identifier quoting problem causing errors with dbplyr 2.0.0 (#48)
* Removed deprecated functions from tests (#42)

# implyr 0.3.0

* Added more SQL translations including stringr and lubridate functions
* Updated for compatibility with dplyr 0.8.3
* Fixed errors when `na.rm = TRUE` is specified in some aggregate functions
* Added `src_databases()` function (#36)
* Made minor bugfixes and improvements

# implyr 0.2.4

* Made minor bugfixes and improvements
* Added rJava to Suggests (#26)

# implyr 0.2.3

* Updated for compatibility with dplyr 0.7.4
* Added tidyselect to Imports
* Required newer versions of Depends and Imports packages
* Added experimental support for complex types (`ARRAY`, `MAP`, `STRUCT`)
* Made bugfixes and improvements

# implyr 0.2.2

* Moved DBI from Imports to Depends (#9)
* Made minor bugfixes and improvements

# implyr 0.2.1

* Updated for compatibility with dbplyr 1.1.0
* Added and changed some SQL translations
* Enabled auto-disconnect by default
* Made minor bugfixes and improvements

# implyr 0.2.0

* Updated for compatibility with dplyr 0.7.0 (#5)
* Added automated tests (#6)
* Documented how to use `in_schema()` to specify the database containing the table (#2, #3)
* Made minor bugfixes and improvements
