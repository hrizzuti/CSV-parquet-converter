scalaVersion := "2.11.12"
scalacOptions += "-Ypartial-unification"
val excludeSlf4jBinding = ExclusionRule("org.slf4j", "slf4j-log4j12")
val excludeSlf4jApiBinding = ExclusionRule("org.slf4j", "slf4j-api")
val excludeLog4jBinding = ExclusionRule("log4j", "log4j")

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.5"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.5"
