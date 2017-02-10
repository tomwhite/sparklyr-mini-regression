# This is a sparklyr extension for running linear regressions over windows of
# timeseries data.
#
# You can learn more about sparklyr at:
#
#   http://spark.rstudio.com/
#

#' @import sparklyr
#' @export
run_mini_regressions <- function(dataset, groupingColumn, x, y) {
  # normalize whatever we were passed (e.g. a dplyr tbl) into a DataFrame
  df <- spark_dataframe(dataset)

  # get the underlying connection so we can create new objects
  sc <- spark_connection(df)

  sc %>%
    invoke_new("com.cloudera.datascience.sparklyr.extensions.MiniRegression") %>%
    invoke("run", df, groupingColumn, x, y)
}
