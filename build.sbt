name := "ScalaHBase Learning"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.2"

resolvers += Resolver.mavenLocal

sources in (Compile, doc) := Seq.empty
