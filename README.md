## Learning to develop spark apps with scala
* Create an "sbt-based Scala project" in the Intellij. This will create a proper project structure as well
* Add the following dependencies to the build.sbt file
```
version := "0.1"

scalaVersion := "2.12.8"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0"
// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.0"
```
* Create necessary packages inside the src/main/scala folder and inside that create scala classes/object.
* If this is a learning exercise, make sure to create a main function under each class and run the file
