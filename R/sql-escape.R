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

# Escape quotes with a backslash instead of doubling
sql_quote <- function(x, quote) {
  y <- gsub(quote, paste0("\\", quote), x, fixed = TRUE)
  y <- paste0(quote, y, quote)
  y[is.na(x)] <- "NULL"
  names(y) <- names(x)
  y
}
