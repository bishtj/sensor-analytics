name := "sensor-analytics"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion = "2.4.5"
val catsVersion = "2.0.0"

val scalaTestVersion = "3.0.0"
val sparkTestingBaseVersion = "2.4.5_0.14.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
//  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion,
//  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",

  "org.typelevel" %% "cats-core" % catsVersion,
  "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
  "com.holdenkarau" %% "spark-testing-base" % sparkTestingBaseVersion % "test"
)
// Parallel execution of spark tests not supported, as they share same spark session
parallelExecution in ThisBuild := false

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

// META-INF discarding for the FAT JAR
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  artifact.name + "_" + sv.binary + "-" + sparkVersion + "_" + module.revision + "." + artifact.extension
}

