/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.amazonaws.services.kinesisanalytics


import org.apache.flink.streaming.api.scala._
import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisProducer
import org.apache.flink.streaming.connectors.kinesis.config._

import java.io.IOException
import java.util
import java.util.Properties


/**
 * Skeleton for a Flink Streaming Job.
 *
 * For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
object StreamingJob {
  private val region = "us-east-1"
  private val inputStreamName = "ExampleInputStream"
  private val outputStreamName = "ExampleOutputStream"

  import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime
  import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer
  import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisProducer
  import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants
  import java.io.IOException
  import java.util

  private def createSourceFromStaticConfig(env: StreamExecutionEnvironment) = {
    val inputProperties = new Properties
    inputProperties.setProperty(AWSConfigConstants.AWS_REGION, region)
    inputProperties.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "LATEST")
    env.addSource(new FlinkKinesisConsumer(inputStreamName, new SimpleStringSchema, inputProperties))
  }

  @throws[IOException]
  private def createSourceFromApplicationProperties(env: StreamExecutionEnvironment) = {
    val applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties
    env.addSource(new FlinkKinesisConsumer(inputStreamName, new SimpleStringSchema, applicationProperties.get("ConsumerConfigProperties")))
  }

  private def createSinkFromStaticConfig = {
    val outputProperties = new Properties
    outputProperties.setProperty(AWSConfigConstants.AWS_REGION, region)
    outputProperties.setProperty("AggregationEnabled", "false")
    val sink = new FlinkKinesisProducer(new SimpleStringSchema, outputProperties)
    sink.setDefaultStream(outputStreamName)
    sink.setDefaultPartition("0")
    sink
  }

  @throws[IOException]
  private def createSinkFromApplicationProperties = {
    val applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties
    val sink = new FlinkKinesisProducer(new SimpleStringSchema, applicationProperties.get("ProducerConfigProperties"))
    sink.setDefaultStream(outputStreamName)
    sink.setDefaultPartition("0")
    sink
  }


  def main(args: Array[String]) {
    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    /* if you would like to use runtime configuration properties, uncomment the lines below
   * val input = createSourceFromApplicationProperties(env)
   */

    val input = createSourceFromStaticConfig(env)


    /* if you would like to use runtime configuration properties, uncomment the lines below
    * input.addSink(createSinkFromApplicationProperties())
    */

    input.addSink(createSinkFromStaticConfig)

    // execute program
    env.execute("Flink Streaming Scala API Skeleton")
  }
}
