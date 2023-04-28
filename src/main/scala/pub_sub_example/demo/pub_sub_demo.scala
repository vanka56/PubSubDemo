package pub_sub_example.demo

import java.nio.charset.StandardCharsets
import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.pubsub.{PubsubUtils, SparkGCPCredentials}
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.{Seconds, StreamingContext}


import org.apache.spark.rdd.RDD
import org.json4s._
import org.json4s.jackson.JsonMethods._
import java.util.Calendar
import java.text.SimpleDateFormat
import scala.util.matching.Regex


object PubSubDemo {

  var projectID: String = null
  var payloadType: String = null

  val dT = Calendar.getInstance()

  def transformInput(input: RDD[String])= {
    // val rddMap = input.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_).map{s => Map(s._1 -> s._2)}
    val rddMap = input.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_).map(Map(_))
    persistData[Int](rddMap)

  }

  def getCurrentHour() =  dT.get(Calendar.HOUR_OF_DAY).toString

  def getCurrentDate() = {
    val form = new SimpleDateFormat("yyyy-MM-dd")
    val currentDate = form.format(dT.getTime())
    currentDate
}

  def persistData[T](input: RDD[Map[String,T]]) = {
    input.collect().foreach(println)
    input.saveAsObjectFile(s"gs://$projectID/PubSubDemo/Persist/ds=$getCurrentDate/$payloadType/hr=$getCurrentHour/")
  }

  def mapInputToJson(input: RDD[String]) = {
      val parsedMessages = input.mapPartitions { partition =>
      implicit val formats: DefaultFormats.type = DefaultFormats

      // Define a regular expression to match unwanted characters at the beginning of the string
      val regex: Regex = "^[bB]?[\"']".r

      partition.map { message =>
        val cleanedMessage = regex.replaceFirstIn(message, "") // Remove unwanted characters
        parse(cleanedMessage.replace("\'", "\"").replace("None","null")).extract[Map[String, Any]]
      }

      }
    // Process or analyze the parsed messages
    persistData[Any](parsedMessages)
    
  }


  def main(args: Array[String]): Unit = {

    if(args.length != 6){
      System.out.println("Arguments expected : projectID, topicName, subscriptionName, payloadType, slidingInterval, chekpointDir")
      System.exit(1)
    }

    val Seq(projectID, topicName, subscriptionName, payloadType, slidingInterval, chekpointDir) = args.toSeq

    this.projectID = projectID
    this.payloadType = payloadType

    val credentials = SparkGCPCredentials.builder.build()
    // val projectId = args
    // val topicName = "your-topic-name"
    // val subscriptionName = "your-subscription-name"

    // Create Spark configuration
    val conf = new SparkConf().setAppName("PubSubDemo")
    val ssc = new StreamingContext(conf, Seconds(slidingInterval.toInt))

    val yarnTags = conf.get("spark.yarn.tags")
    val jobId = yarnTags.split(",").filter(_.startsWith("dataproc_job")).head
    ssc.checkpoint(chekpointDir + "/" + jobId)


    val messagesStream= PubsubUtils
      .createStream(
        ssc,
        projectID,
        None,
        subscriptionName,  // Cloud Pub/Sub subscription for incoming tweets
        SparkGCPCredentials.builder.build(), 
        StorageLevel.MEMORY_AND_DISK_SER_2
      )
      .map(message => new String(message.getData(), StandardCharsets.UTF_8))

    if(payloadType == "text"){
      val parsedMessages= messagesStream.window(Seconds(30), Seconds(30)).foreachRDD(transformInput(_))
      // persistData(parsedMessages, projectID, payloadType)
  }

    else
    {
        messagesStream.window(Seconds(30), Seconds(30)).foreachRDD(mapInputToJson _)

    }
        // Start the computation
        ssc.start()
        ssc.awaitTermination()
  }
}
