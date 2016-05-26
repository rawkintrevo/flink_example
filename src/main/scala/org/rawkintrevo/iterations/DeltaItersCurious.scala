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

import scala.util.Random.nextInt

object DeltaItersCurious {
   def main(args: Array[String]) {
     // set up the execution environment
     val env = ExecutionEnvironment.getExecutionEnvironment
     val maxIterations = 10 //args(0).toInt
     val dsSize = 100 //args(1).toInt

     val keyPosition = 0  // NEW - the part of the tuple you're gonna hit, btw - gotta have tuples

     // Create a DataSet of (index, randomInt)
     val inputData:  DataSet[(Long, Int)] = env.fromCollection(List.fill(dsSize)(nextInt(maxIterations))).zipWithIndex
     val stepSize: Int = 1

     val initialWorkset: DataSet[(Long,Int)] = env.fromCollection(List.fill(dsSize)(0)).zipWithIndex
     val initialSolutionSet: DataSet[(Long,Int)] = env.fromCollection(List.fill(dsSize)(0)).zipWithIndex

     // A wierd way to find the value of a dataset...
     val result = initialSolutionSet.iterateDelta(initialWorkset, maxIterations, Array(keyPosition)) {
       (solution, workset) =>
         val deltas = workset.join(inputData).where(0).equalTo(0)
                              .filter( o => o._1._2 != o._2._2 )  // sign seems to matter?  0s if greater than , 10s if less than unless equal
                              .map(o => o._1)

         val solutionUpdates = solution.join(deltas).where(0).equalTo(0) {
           (current, delta) => (current._1, current._2 + stepSize)
         }


         (solutionUpdates, deltas)
     }


     inputData.join(result).where(0).equalTo(0).writeAsCsv("/home/trevor/myProjects/flink_iterations/delta1.csv")

     // execute program
     env.execute("Flink Delta Iteration Example")
   }
 }
