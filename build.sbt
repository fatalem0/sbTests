ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "sberbankTests",
    libraryDependencies ++= Seq(
        "org.apache.spark" %% "spark-core" % "3.3.0",
        "org.apache.spark" %% "spark-sql" % "3.3.0",
        "com.crealytics" %% "spark-excel" % "3.2.1_0.17.1",
        "org.postgresql" % "postgresql" % "42.4.0"
    )
  )
