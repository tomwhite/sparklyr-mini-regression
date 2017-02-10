package com.cloudera.datascience.sparklyr.extensions

import org.apache.commons.math3.stat.regression.SimpleRegression
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

class MiniRegression extends Serializable {
  def fit(id: String, pairs: Seq[Tuple2[Double, Double]]): (String, Double) = {
    val reg = new SimpleRegression()
    pairs.foreach{ case (x, y) => reg.addData(x, y) }
    (id, reg.getSlope())
  }

  def getBetaValues(id: String, pairs: Iterator[Tuple2[Double, Double]], window: Int): Iterator[(String, Double)] = {
    pairs.sliding(window).map(fit(id, _))
  }

  def run(dataset: Dataset[Row], groupingColumn: String, x: String, y: String): Dataset[Row] = {
    val sqlContext = new org.apache.spark.sql.SQLContext(dataset.rdd.context)
    import sqlContext.implicits._
    dataset
      .groupByKey(_.getAs[String](groupingColumn))
      .flatMapGroups{
        case (k, rows) => getBetaValues(k, rows.map{ case (row) => (row.getAs[Double](x),
          row.getAs[Double](y)) }, 365)
      }
      .toDF(groupingColumn, groupingColumn + "_BETA")
  }
}