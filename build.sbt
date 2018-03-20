organization := "com.github.biopet"
organizationName := "Biopet"

startYear := Some(2018)

name := "BamToRawVcf"
biopetUrlName := "bamtorawvcf"

biopetIsTool := true

mainClass in assembly := Some(
  s"nl.biopet.tools.${name.value.toLowerCase()}.${name.value}")

developers += Developer(id = "ffinfo",
                        name = "Peter van 't Hof",
                        email = "pjrvanthof@gmail.com",
                        url = url("https://github.com/ffinfo"))

scalaVersion := "2.11.12"

libraryDependencies += "com.github.biopet" %% "spark-utils" % "0.4-SNAPSHOT" changing ()
libraryDependencies += "com.github.biopet" %% "tool-utils" % "0.4-SNAPSHOT" changing ()
libraryDependencies += "com.github.biopet" %% "tool-test-utils" % "0.2.2" % Test
