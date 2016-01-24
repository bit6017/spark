/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.scheduler.cluster

import org.apache.hadoop.yarn.util.RackResolver
import org.apache.log4j.{Level, Logger}

import org.apache.spark._
import org.apache.spark.scheduler.TaskSchedulerImpl
import org.apache.spark.util.Utils

/**
 * YarnScheduler继承自TaskSchedulerImpl，并且以SparkContext作为构造参数
 * @param sc
 */
private[spark] class YarnScheduler(sc: SparkContext) extends TaskSchedulerImpl(sc) {

  // RackResolver logs an INFO message whenever it resolves a rack, which is way too often.
  if (Logger.getLogger(classOf[RackResolver]).getLevel == null) {
    Logger.getLogger(classOf[RackResolver]).setLevel(Level.WARN)
  }

  // By default, rack is unknown

  /**
   * getRackForHost在TaskSchedulerImpl中默认返回None，也就是Spark Standalone模式是没有机架感知能力的？
   * 而，基于HADOOP的YARN是有机架感知能力的
   * @param hostPort
   * @return
   */
  override def getRackForHost(hostPort: String): Option[String] = {
    //解析host:post，得到host部分的信息
    val host = Utils.parseHostPort(hostPort)._1

    //调用HADOOP YARN的RackResolver.resolve方法获取指定host的机架信息
    /***问题：sc的hadoopConfiguration是何时注入的？SparkContext通过调用 SparkHadoopUtil.get.newConfiguration(SparkConf)获得的*/
    Option(RackResolver.resolve(sc.hadoopConfiguration, host).getNetworkLocation)
  }
}
