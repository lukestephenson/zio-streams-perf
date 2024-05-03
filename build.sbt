ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.4.1"

//val zioVersion = "2.0.18+16-9a15165a-SNAPSHOT"
val zioVersion = "2.1-RC1"
//val zioVersion = "2.0.21"

val kyoVersion = "0.9.2"

val monixVersion = "3.4.1"

lazy val root = (project in file("."))
  .settings(
    name := "zio-streams-perf",
    libraryDependencies ++= Seq(
      // ZIO
      "dev.zio" %% "zio-streams" % zioVersion,
      "dev.zio" %% "zio-test" % zioVersion % Test,
      // zio-test-sbt breaks running tests in intellij.
      "dev.zio" %% "zio-test-sbt" % zioVersion % Test,
      // Monix - only for benchmarks
      "io.monix" %% "monix" % monixVersion,
      "io.monix" %% "monix-reactive" % monixVersion,
      "io.monix" %% "monix-execution" % monixVersion,
      // KYO
      "io.getkyo" %% "kyo-core" % kyoVersion
    )
  )

testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")

//libraryDependencies += "dev.zio" %% "zio-profiling" % "0.2.1"
//libraryDependencies += compilerPlugin("dev.zio" %% "zio-profiling-tagging-plugin" % "0.2.1")

enablePlugins(JmhPlugin)