name := "jobbroker-dao"
organization := "com.ruimo"
scalaVersion := "2.12.6"

publishTo := Some(
  Resolver.file(
    "jobbroker-client-scala",
    new File(Option(System.getenv("RELEASE_DIR")).getOrElse("/tmp"))
  )
)

resolvers += "ruimo.com" at "http://static.ruimo.com/release"

libraryDependencies += "com.ruimo" %% "jobbroker-common" % "1.0-SNAPSHOT"
libraryDependencies += "com.typesafe.play" %% "anorm" % "2.5.3"
libraryDependencies += "org.liquibase" % "liquibase-core" % "3.6.2"
libraryDependencies += "org.postgresql" % "postgresql" % "42.2.2"

libraryDependencies += "org.specs2" %% "specs2-core" % "4.3.2" % "test"
libraryDependencies += "com.h2database"  %  "h2" % "1.4.197" % "test"

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")
