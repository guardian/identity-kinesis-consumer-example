package com.gu.identity

import java.net.InetAddress
import java.util.{Base64, UUID}

import net.liftweb.json.{DefaultFormats, Serialization}

import scala.collection.JavaConversions._

import com.amazonaws.auth._
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.{IRecordProcessorFactory, IRecordProcessor}
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{Worker, KinesisClientLibConfiguration}
import com.amazonaws.services.kinesis.clientlibrary.types.{ProcessRecordsInput, InitializationInput, ShutdownInput}

case class Config(kinesisStreamName: String = "my-stream", kinesisEndPoint: String = "kinesis.eu-west-1.amazonaws.com")

object IdentityKinesisConsumerExample extends App {

  val usage = """
    Usage: run --streamName <stream name> [--endPoint <end-point URL>]
              """
  override def main(args: Array[String]) {
    val parser = new scopt.OptionParser[Config]("scopt") {
      head("identity-kinesis-consumer-example", "0.1")
      opt[String]('s', "streamName") action { (x, c) =>
        c.copy(kinesisStreamName = x)
      } text "Kinesis stream name"
      opt[String]('e', "endPoint") action { (x, c) =>
        c.copy(kinesisEndPoint = x)
      } text "Kinesis end-point"
    }

    parser.parse(args, Config()) match {
      case Some(config) => {

        println(s"Got Kinesis stream name = ${config.kinesisStreamName}")
        println(s"Got Kinesis end-point = ${config.kinesisEndPoint}")

        val workerId = InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID()

        val kinesisConfig = new KinesisClientLibConfiguration(
          "identity-kinesis-consumer-example",
          config.kinesisStreamName,
          new DefaultAWSCredentialsProviderChain(),
          workerId
        ).withKinesisEndpoint(config.kinesisEndPoint)

        val recordProcessorFactory = new RecordProcessorFactory()

        val worker = new Worker.Builder()
          .recordProcessorFactory(recordProcessorFactory)
          .config(kinesisConfig)
          .build()

        worker.run()

      }
      case None => System.exit(0)
    }
  }
}


class RecordProcessorFactory extends IRecordProcessorFactory {

  def createProcessor(): IRecordProcessor = new RecordProcessor()

}

class RecordProcessor extends IRecordProcessor {

  import net.liftweb.json._
  implicit val formats = DefaultFormats

  case class UserChangedMessage(userId: String)

  def initialize(initialisationInput: InitializationInput) = {
    println(s"Shard ID = ${initialisationInput.getShardId}")
  }

  def processRecords(processRecordsInput: ProcessRecordsInput) = {
    processRecordsInput.getRecords.foreach{ record => {
      val bytes = new Array[Byte](record.getData().remaining())
      record.getData().get(bytes)
      val data = new String(bytes)
      val json = parse(data)
      val event = json.extract[UserChangedMessage]
      println(s"\n\n=====\nGot event for user ID = ${event.userId}\n=====\n\n")
    }}

  }

  def shutdown(shutdownInput: ShutdownInput) = {

  }
}