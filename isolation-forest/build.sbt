// Project settings
name := "IsolationForestWrapper"
version := "1.0"
scalaVersion := "2.12.15"

// Spark and Hadoop dependencies
val sparkVersion = "3.2.1"

// Add dependencies
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "com.linkedin.isolation-forest" % "isolation-forest_3.2.0_2.12" % "3.0.2",
  "com.typesafe" % "config" % "1.4.1"
)

// Add Maven Central repository
resolvers += Resolver.mavenCentral

// Assembly settings
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case _                        => MergeStrategy.first
}
