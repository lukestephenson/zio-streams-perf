ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.3.1"

//val zioVersion = "2.0.18+16-9a15165a-SNAPSHOT"
val zioVersion = "2.1-RC1"
//val zioVersion = "2.0.21"

resolvers += Resolver.mavenLocal

lazy val root = (project in file("."))
  .settings(
    name := "zio-streams-perf",
    libraryDependencies ++= Seq(
      // ZIO
      "dev.zio" %% "zio-streams" % zioVersion,
      "dev.zio" %% "zio-test" % zioVersion % Test,
      "dev.zio" %% "zio-test-sbt" % zioVersion % Test,
    )
  )

//libraryDependencies += "dev.zio" %% "zio-profiling" % "0.2.1"
//libraryDependencies += compilerPlugin("dev.zio" %% "zio-profiling-tagging-plugin" % "0.2.1")

enablePlugins(JmhPlugin)