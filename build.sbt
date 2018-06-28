organization := "com.github.biopet"
organizationName := "Biopet"

startYear := Some(2018)

name := "TenxKit"
biopetUrlName := "10xkit"

biopetIsTool := true

mainClass in assembly := Some(
  s"nl.biopet.tools.${name.value.toLowerCase()}.${name.value}")

developers += Developer(id = "ffinfo",
                        name = "Peter van 't Hof",
                        email = "pjrvanthof@gmail.com",
                        url = url("https://github.com/ffinfo"))

fork in Test := true

scalaVersion := "2.11.12"

libraryDependencies += "com.github.biopet" %% "spark-utils" % "0.4-SNAPSHOT" changing ()
libraryDependencies += "com.github.biopet" %% "tool-utils" % "0.4"
libraryDependencies += "com.github.biopet" %% "tool-test-utils" % "0.2.2" % Test

libraryDependencies += "colt" % "colt" % "1.2.0"

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.8.7"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.0" % Provided
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.0" % Provided
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.3.0" % Provided
libraryDependencies += "org.bdgenomics.adam" %% "adam-core-spark2" % "0.24.0" exclude ("org.apache.hadoop", "hadoop-client")
