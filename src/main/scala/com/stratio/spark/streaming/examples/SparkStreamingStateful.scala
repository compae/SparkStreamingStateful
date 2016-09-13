package com.stratio.spark.streaming.examples

import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Minutes, Seconds, State, StateSpec, StreamingContext, Time}
import org.apache.spark.{Logging, SparkConf, SparkContext}
import SparkStreamingStatefulFunctions._

object SparkStreamingStateful extends App with Logging {

  private var newContextCreated = false

  def creatingFunc(): StreamingContext = {
    /**
     * Spark Configuration and Create Spark Contexts
     */
    val sparkConf = new SparkConf().setAppName("sparkStreamingExamples").setMaster("local[*]")
    val sparkContext = SparkContext.getOrCreate(sparkConf)
    val sqlContext = SQLContext.getOrCreate(sparkContext)
    val ssc = new StreamingContext(sparkContext, Seconds(5))
    val stream = ssc.receiverStream(new DummyReceiver(10))

    stream.print()

    /**
     * SQL over Streaming
     */
    stream.foreachRDD { rdd =>
      sqlContext.createDataFrame(rdd).registerTempTable("sqlinstreaming")
      val query = sqlContext.sql("select * from sqlinstreaming where key > 5")
      query.show()
    }

    /**
     * StateFull with Map with State
     */
    //mapWithStateExecution(sparkContext, stream)

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

object SparkStreamingStatefulFunctions {

  /**
   * Function used in mapWithState
   */
  def trackStateFunc(batchTime: Time, key: Int, value: Option[Int], state: State[List[Int]]): Option[Event] = {
    value.foreach(data => state.getOption() match {
      case Some(lastState) => state.update(data :: lastState)
      case None => state.update(List(data))
    })
    val sumValues = state.get().sum
    if (sumValues > 50) {
      state.remove()
      Some(Event(key, sumValues))
    }
    else None
  }

  def mapWithStateExecution(@transient sparkContext: SparkContext, stream: DStream[Event]): Unit = {
    val sqlContext = SQLContext.getOrCreate(sparkContext)
    val keyValueStream = stream.map(event => (event.key, event.value))
    val stateSpec = StateSpec.function(trackStateFunc _)
      .numPartitions(3)
      .timeout(Seconds(60))

    val statefulDStream = keyValueStream.mapWithState(stateSpec)

    // A snapshot of the state for the current batch. This dstream contains one entry per key.
    val stateSnapshotStream = statefulDStream.stateSnapshots()
    stateSnapshotStream.print()

    //print the output on each batch.
    statefulDStream.print()

    //print the values based on a query
    statefulDStream.foreachRDD { rdd =>
      sqlContext.createDataFrame(rdd).registerTempTable("statefulresult")
      val query = sqlContext.sql("select * from statefulresult where key > 5")
      query.show()
    }
  }
}
