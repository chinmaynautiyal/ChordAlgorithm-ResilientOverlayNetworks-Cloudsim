name := "nautiyal_raj_courseproject"

version := "0.1"

scalaVersion := "2.13.1"


libraryDependencies ++= Seq("com.typesafe" % "config" % "1.3.4",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2",
  //akka dependency

  "com.typesafe.akka" %% "akka-http"   % "10.1.11",
  "com.typesafe.akka" %% "akka-stream" % "2.5.26",
  //"com.typesafe.akka" %% "akka-actor" % "2.6.1",
  "commons-codec" % "commons-codec" % "1.13",
  "org.scalactic" %% "scalactic" % "3.0.8",
  "org.scalatest" %% "scalatest" % "3.0.8" % "test"

)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}