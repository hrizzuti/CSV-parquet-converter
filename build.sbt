scalaVersion := "2.12.10"
scalacOptions += "-Ypartial-unification"
val awsPackagesVersion = "1.11.909"
val hadoopVersion = "3.2.0"
val excludeSlf4jBinding = ExclusionRule("org.slf4j", "slf4j-log4j12")
val excludeSlf4jApiBinding = ExclusionRule("org.slf4j", "slf4j-api")
val excludeLog4jBinding = ExclusionRule("log4j", "log4j")

libraryDependencies += "com.amazonaws" % "aws-java-sdk-sts" % awsPackagesVersion
libraryDependencies += "com.amazonaws" % "aws-java-sdk-bundle" % awsPackagesVersion
libraryDependencies += "com.amazonaws" % "aws-java-sdk-s3" % awsPackagesVersion

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % hadoopVersion excludeAll(excludeSlf4jBinding, excludeSlf4jApiBinding, excludeLog4jBinding)
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % hadoopVersion

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.1"
