name := "college_vine"
version := "1.0"
scalaVersion := "2.13.8"

val sparkVersion = "3.2.1"

// Note the dependencies are provided
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion withSources()
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion withSources()
libraryDependencies += "com.typesafe" % "config" % "1.4.2"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.11" % Test

// Do not include Scala in the assembled JAR
//assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
