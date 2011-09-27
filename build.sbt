seq(assemblySettings: _*)

resolvers += "clojars" at "http://clojars.org/repo"

libraryDependencies ++= Seq(
    "storm" % "storm" % "0.5.2",
    "commons-codec" % "commons-codec" % "1.4",
    "org.twitter4j" % "twitter4j-core" % "2.2.4",
    "org.twitter4j" % "twitter4j-stream" % "2.2.4",
    "org.apache.lucene" % "lucene-core" % "3.4.0",
    "org.apache.lucene" % "lucene-analyzers" % "3.4.0",
    "redis.clients" % "jedis" % "2.0.0"
)

scalaVersion := "2.9.1"
