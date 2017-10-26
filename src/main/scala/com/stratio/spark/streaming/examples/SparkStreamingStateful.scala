package com.stratio.spark.streaming.examples

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object SparkStreamingStateful extends App with Logging {

  private var newContextCreated = false

  def creatingFunc(): StreamingContext = {
    /**
      * Spark Configuration and Create Spark Contexts
      */
    val sparkConf = new SparkConf().setAppName("sparkStreamingExamples").setMaster("local[*]")
    val sparkContext = SparkContext.getOrCreate(sparkConf)
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val ssc = new StreamingContext(sparkContext, Seconds(10))
    val stream = ssc.receiverStream(new DummyReceiver(2))

    /**
      * SQL over Streaming
      */
    stream.foreachRDD { rdd =>
      sparkSession.createDataFrame(rdd).createOrReplaceTempView("sqlinstreaming")
      val query = sparkSession.sql("select * from sqlinstreaming where key = 'blue'")
      query.show()
    }

    /**
      * StateFull with Map with State
      */
    import MapWithStateFunctions._
    mapWithStateExecution(sparkSession, stream)

    ssc.remember(Minutes(1)) // To make sure data is not deleted by the time we query it interactively
    ssc.checkpoint("checkpoint")

    println("Creating function called to create new StreamingContext")
    newContextCreated = true
    ssc
  }

  StreamingContext.getActive.foreach {
    _.stop(stopSparkContext = false)
  }

  val ssc = StreamingContext.getActiveOrCreate(creatingFunc)
  if (newContextCreated)
    println("New context created from currently defined creating function")
  else println("Existing context running or recovered from checkpoint, may not be running currently defined creating function")

  ssc.start()

  ssc.awaitTerminationOrTimeout(15000)
}

