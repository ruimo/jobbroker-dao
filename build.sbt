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

libraryDependencies += "com.ruimo" %% "jobbroker-common" % "1.0"
libraryDependencies += "org.playframework.anorm" %% "anorm" % "2.6.2"
libraryDependencies += "org.liquibase" % "liquibase-core" % "3.6.2"
libraryDependencies += "org.postgresql" % "postgresql" % "42.2.2"

libraryDependencies += "org.specs2" %% "specs2-core" % "4.3.3" % Test
libraryDependencies += "com.h2database"  %  "h2" % "1.4.197" % Test

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")
