# Using Scala UDFs for Data Linkage in Pyspark

an extension of 

## Using a Scala UDF Example
https://github.com/ONSBigData/scala_udf_example
from @philip-lee-ons


Phillip has created an example of a UDF defined in Scala, callable from PySpark,
that wraps a call to JaroWinklerDistance from Apache commons.

My intention is to add more string distances and similarities from Apache commons for use in fuzzy matching in Pyspark



## Usage

To build the Jar:

    mvn package
    
To add the jar to PySpark set the following config:

    spark.driver.extraClassPath /path/to/jarfile.jar
    spark.jars /path/to/jarfile.jar
    
To register the function with PySpark:

```python
sqlContext = SQLContext(spark.sparkContext)
sqlContext.registerJavaFunction('jaro_winkler', 'uk.gov.ons.mdr.linkage.JaroWinklerDistance',\ 
                                pyspark.sql.types.DoubleType())
```
