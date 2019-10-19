# Using Scala UDFs for Data Linkage in Pyspark

an extension of 

## Using a Scala UDF Example
https://github.com/ONSBigData/scala_udf_example
from @philip-lee-ons


Phillip has created an example of a UDF defined in Scala, callable from PySpark,
that wraps a call to JaroWinklerDistance from Apache commons.

My intention is to add [more text distance and similarity metrics from Apache Commons](https://commons.apache.org/proper/commons-text/apidocs/org/apache/commons/text/similarity/package-summary.html) 
for use in fuzzy matching in Pyspark

## Progress

* JaroWinklerSimilarity has been used instead of JaroWinklerDistance 
* Added CosineDistance and JaccardSimilarity from Apache Commons





## Usage

To build the Jar:

    mvn package
    
To add the jar to PySpark set the following config:

    spark.driver.extraClassPath /path/to/jarfile.jar
    spark.jars /path/to/jarfile.jar
    
To register the function with PySpark 2.3.1:

```python

spark.udf.registerJavaFunction('jaro_winkler', 'uk.gov.moj.dash.linkage.JaroWinklerDistance',\ 
                                pyspark.sql.types.DoubleType())
```


###  Maven? How can I install this on my macbook?

Maven is written in Java (and primarily used for building JVM programs). 
* the major Maven prerequisite is a Java JDK. 
If you dont have one ,installing either OpenJDK or [AWS Correto JDK](https://aws.amazon.com/blogs/opensource/amazon-corretto-no-cost-distribution-openjdk-long-term-support/) is recommended.Oracle JDK is not anymore.



* To install Maven on Mac OS X operating system, download the latest version from the Apache Maven site, select the Maven binary tar.gz file, for example: apache-maven-3.3.9-bin.tar.gz to to Downloads/ 

    * Extract the archive with `tar -xzfv apache-maven-3.3.9-bin.tar.gz`

    * On the shell prompt: `sudo chown -R root:wheel Downloads/apache-maven*` for fixing permissions.

    * On the shell prompt: `sudo mv Downloads/apache-maven* /opt/apache-maven`

    * Edit your .bashprofile with `nano ~/.bash_profile` and add  `export PATH=$PATH:/opt/apache-maven/bin` there
    
    
* To install Maven on a Debian based Linux distribution (such as Ubuntu) there is an easier way: `sudo apt-get install maven`
 
* Test that everything has been installed fine by running `java -version` and `mvn -version`  on your bash prompt

* Then `mvn package`








