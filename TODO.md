
To add to v0.0.6


1)  Add the alternate encoding of Double Metaphone from Apache Commons



2)  Add a UDF to do something similar to :




`val loopUDF = udf { x: Seq[String] => for (a <- x; b <-x) yield (a,b) }`




but for 2  Seq[String]  inputs.
