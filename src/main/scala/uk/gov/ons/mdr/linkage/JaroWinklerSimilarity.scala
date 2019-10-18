/**
  * Simple Scala wrapper to turn an existing string similarity function into a UDF
  */
package uk.gov.ons.mdr.linkage

import org.apache.spark.sql.api.java.UDF2
import org.apache.commons.text.similarity

class JaroWinklerSimilarity extends UDF2[String, String, Double] {
  override  def call(left: String, right: String): Double = {
    // This has to be instantiated here (i.e. on the worker node)
    val distance = new similarity.JaroWinklerDistance()
    distance(left, right)
  }
}

object JaroWinklerSimilarity {
  def apply(): JaroWinklerSimilarity = {
    new JaroWinklerSimilarity()
  }
}
