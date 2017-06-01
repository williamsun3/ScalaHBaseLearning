name := "ScalaHBase Learning"

version := "1.0"

scalaVersion := "2.12.2"

libraryDependencies ++= Seq(
  "com.google.guava" % "guava" % "15.0",
  "org.apache.hadoop" % "hadoop-common" % "2.6.0",
  "org.apache.hbase" % "hbase-common" % "1.0.0",
  "org.apache.hbase" % "hbase-client" % "1.0.0"
)

resolvers += Resolver.mavenLocal

sources in (Compile, doc) := Seq.empty
