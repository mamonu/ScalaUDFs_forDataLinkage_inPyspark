/**
  * Simple Scala wrappers to turn an existing string similarity functions into UDFs
  */
package uk.gov.moj.dash.linkage


import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.api.java.UDF2
import org.apache.spark.sql.api.java.UDF1


import org.apache.commons.text.similarity
import org.apache.commons.codec.language




class DoubleMetaphone extends UDF1[String, String] {
  override  def call(input: String): String = {
    // This has to be instantiated here (i.e. on the worker node)
   

    val  m = new language.DoubleMetaphone()
    m.doubleMetaphone(input)
  }
}

object DoubleMetaphone {
  def apply(): DoubleMetaphone = {
    new DoubleMetaphone()
  }
}


class QgramTokeniser extends UDF1[String, String] {
  override  def call(input: String): String = {
    // This has to be instantiated here (i.e. on the worker node)
   
    input.sliding(2).toList.mkString(" ")  
     
  }
}

object QgramTokeniser {
  def apply(): QgramTokeniser = {
    new QgramTokeniser()
  }
}







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


class JaccardSimilarity extends UDF2[String, String, Double] {
  override  def call(left: String, right: String): Double = {
    // This has to be instantiated here (i.e. on the worker node)
    val distance = new similarity.JaccardSimilarity()
    distance(left, right)
  }
}

object JaccardSimilarity {
  def apply(): JaccardSimilarity = {
    new JaccardSimilarity()
  }
}



class CosineDistance extends UDF2[String, String, Double] {
  override  def call(left: String, right: String): Double = {
    // This has to be instantiated here (i.e. on the worker node)
    val distance = new similarity.CosineDistance()
    distance(left, right)
  }
}

object CosineDistance {
  def apply(): CosineDistance = {
    new CosineDistance()
  }
}