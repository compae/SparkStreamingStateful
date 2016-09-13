package com.stratio.spark.streaming.examples

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, State, StateSpec, Time}

object MapWithStateFunctions {

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