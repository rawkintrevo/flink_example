package org.rawkintrevo.iterations

/**
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

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.utils.DataSetUtils

import scala.util.Random._

object SampleDatasets {
   def simple(length: Int, beta1: Double, beta2: Double) = {
     // set up the execution environment
     val env = ExecutionEnvironment.getExecutionEnvironment

     val xs:  DataSet[(Double,Double)] = env.fromCollection(List.fill(length)((nextDouble(), nextDouble())))
     val inputDataSet: DataSet[(Long, (Double, Double, Double))] = xs.map(x => (x._1 * beta1 + x._2 * beta2, x._1, x._2)).zipWithIndex

     inputDataSet
   }
 }
