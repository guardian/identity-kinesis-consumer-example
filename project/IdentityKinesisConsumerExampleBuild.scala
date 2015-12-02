import sbt._
import sbt.Keys._

object IdentityKinesisConsumerExampleBuild extends Build {

  lazy val identityKinesisConsumerExample = Project(
    id = "identity-kinesis-consumer-example",
    base = file("."),
    settings = Project.defaultSettings ++ Seq(
      name := "Identity Kinesis Consumer Example",
      organization := "com.gu.identity",
      version := "0.1-SNAPSHOT",
      scalaVersion := "2.10.2"
      // add other settings here
    )
  )

}
