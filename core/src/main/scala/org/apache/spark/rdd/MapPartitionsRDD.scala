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

package org.apache.spark.rdd

import scala.reflect.ClassTag

import org.apache.spark.{Partition, TaskContext}

/**
 * An RDD that applies the provided function to every partition of the parent RDD.
 *
 * @param prev 该RDD依赖的父RDD
 * @param f 作用于该RDD的一个分区上的计算函数，函数原型是 (TaskContext, Int, Iterator[T]) => Iterator[U]
 * @param preservesPartitioning 是否保留父分区
 * @tparam U 将类型U的元素转换为类型为T的函数
 * @tparam T
 */
private[spark] class MapPartitionsRDD[U: ClassTag, T: ClassTag](
    var prev: RDD[T],
    f: (TaskContext, Int, Iterator[T]) => Iterator[U],  // (TaskContext, partition index, iterator)
    preservesPartitioning: Boolean = false)
  extends RDD[U](prev) {

  override val partitioner = if (preservesPartitioning) firstParent[T].partitioner else None

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  /**
   * 该RDD之上的计算函数，它调用了MapPartitionsRDD传入的构造参数f
   *
   * 此处有递归调用的逻辑：
   * 调用firstParent.iterator方法时，firstParent.iterator又去调用firstParent的compute方法，依次类推产生了pipeline调用
   *
   * @param split 进行计算的分区号，每个Partition实质上是一个index
   * @param context Task上下文
   * @return
   */
  override def compute(split: Partition, context: TaskContext): Iterator[U] =
    f(context, split.index, firstParent[T].iterator(split, context))

  override def clearDependencies() {
    super.clearDependencies()
    prev = null
  }
}
