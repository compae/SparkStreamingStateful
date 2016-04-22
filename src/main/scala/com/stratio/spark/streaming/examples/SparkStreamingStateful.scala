package com.stratio.spark.streaming.examples

import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{StateSpec, State, Time, Minutes, Seconds, StreamingContext}
import org.apache.spark.{Logging, SparkConf, SparkContext}

object SparkStreamingStateful extends App with Logging {

  private var newContextCreated = false

  def trackStateFunc(batchTime: Time, key: Int, value: Option[Int], state: State[List[Int]]): Option[Event] = {
    value.foreach(data => state.getOption() match {
      case Some(lastState) => state.update(data :: lastState)
      case None => state.update(List(data))
    })
    val sumValues = state.get().sum
    if(sumValues > 50){
      state.remove()
      Some(Event(key, sumValues))
    }
    else None
  }


  def creatingFunc(): StreamingContext = {
    /**
     * Spark Configuration and Create Spark Contexts
     */
    val sparkConf = new SparkConf().setAppName("sparkStreamingExamples").setMaster("local[*]")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContext)
    val ssc = new StreamingContext(sparkContext, Seconds(5))

    val stream = ssc.receiverStream(new DummyReceiver(10))
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
