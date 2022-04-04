val scala3Version = "3.1.1"

lazy val root = project
  .in(file("tikv4s"))
  .settings(
    name := "TiDB4s",
    version := "0.1.0-SNAPSHOT",

    scalaVersion := scala3Version,

    libraryDependencies ++= 
      Seq(
      "org.tikv" % "tikv-client-java" % "3.1.0",
      "org.typelevel" %% "cats-core" % "2.7.0",
      "org.typelevel" %% "cats-effect" % "3.3.8",
      "org.typelevel" %% "cats-free" % "2.7.0",
      "org.scalameta" %% "munit" % "0.7.29" % Test, 
      )
  )
