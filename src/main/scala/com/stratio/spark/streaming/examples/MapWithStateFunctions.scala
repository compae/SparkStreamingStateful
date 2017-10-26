package com.stratio.spark.streaming.examples

import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, State, StateSpec, Time}

object MapWithStateFunctions extends Logging {

  /**
    * Function used in mapWithState
    */
  def trackStateFunc(batchTime: Time, key: String, value: Option[Int], state: State[(Long, Double)]): Option[Row] = {

    if (value.isEmpty)
      println("empty")
    if (state.isTimingOut())
      println("timing out")

    value.map { data =>
      state.getOption() match {
        case Some(lastState) =>
          //.update(data :: lastState._1, lastState._2 + 1, lastState._3 + data)
          state.update(lastState._1 + 1, lastState._2 + data)
          Row(key, data * 10)
        case None =>
          //state.update(List(data), 1, data)
          state.update(1, data)
          Row(key, data * 10)
      }
    }
  }

  def mapWithStateExecution(@transient sparkSession: SparkSession, stream: DStream[Event]): Unit = {
    val keyValueStream = stream.map(event => (event.key, event.number))
    val stateSpec = StateSpec.function(trackStateFunc _)
      .numPartitions(3)
      .timeout(Seconds(1))

    val statefulDStream = keyValueStream.mapWithState(stateSpec)

    // A snapshot of the state for the current batch. This dstream contains one entry per key.
    val stateSnapshotStream = statefulDStream.stateSnapshots()

    //print the snapshot stream
    stateSnapshotStream.print()

    //print the output on each batch.
    statefulDStream.print()

    //print the values based on a query
    statefulDStream.foreachRDD { rdd =>
      val schema = StructType(Seq(
        StructField("dimension", StringType),
        StructField("value", IntegerType)
      ))
      sparkSession.createDataFrame(rdd, schema).createOrReplaceTempView("statefulresult")
      val query = sparkSession.sql("select * from statefulresult where dimension = 'red'")
      query.show()
    }
  }
}