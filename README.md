# mumu-spark 基于内存的快速计算框架

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/mumuhadoop/mumu-spark/blob/master/LICENSE)
[![Build Status](https://travis-ci.org/mumuhadoop/mumu-spark.svg?branch=master)](https://travis-ci.org/mumuhadoop/mumu-spark)
[![codecov](https://codecov.io/gh/mumuhadoop/mumu-spark/branch/master/graph/badge.svg)](https://codecov.io/gh/mumuhadoop/mumu-spark)
[![Documentation Status](https://readthedocs.org/projects/mumu-spark/badge/?version=latest)](https://mumu-spark.readthedocs.io/en/latest/?badge=latest)

mumu-spark是一个学习项目，主要通过这个项目来了解和学习spark的基本使用方式和工作原理。mumu-spark主要包括弹性数据集rdd、spark sql、机器学习语言mlib、实时工作流streaming、图形数据库graphx。通过这些模块的学习，初步掌握spark的使用方式。

## spark描述
mumu-spark是一个多功能的快速计算系统，使用spark可以快速的计算大量的数据，比hadoop的mapreduce计算框架快100倍。apache spark是将计算的数据存储到内存中，极大
的提高了计算速度，并且spark可以以master-slave方式部署在集群总，极大的提高了spark的弹性计算。apache spark目前已经发展成集多功能于一体的框架，提供了spark sql、spark streaming、spark mlib、spark graphx等。

## spark 安装方式

## spark core
![spark core](https://raw.githubusercontent.com/mumuhadoop/mumu-spark/master/docs/images/core/spark-ecosystem.png) 

### RDD弹性数据集
RDD 是弹性分布式数据集的简称，是分布式只读且分区的集合对象，这些结合是弹性的，如果数据集一部分丢失了，可以对他们进行重建。具有自动容错、位置感知调度和记录数据的更新。  

RDD 提供了两种基本操作
- transformations 数据转换，如map、flatMap等。
- actions 数据操作，如reduce、count、collect等

#### RDD数据转换
Transformation |	Meaning
//---------------|----------
map(func)	| Return a new distributed dataset formed by passing each element of the source through a function func.
filter(func)	| Return a new dataset formed by selecting those elements of the source on which func returns true.
flatMap(func)	| Similar to map, but each input item can be mapped to 0 or more output items (so func should return a Seq rather than a single item).
mapPartitions(func) |	Similar to map, but runs separately on each partition (block) of the RDD, so func must be of type Iterator<T> => Iterator<U> when running on an RDD of type T.
mapPartitionsWithIndex(func) |	Similar to mapPartitions, but also provides func with an integer value representing the index of the partition, so func must be of type (Int, Iterator<T>) => Iterator<U> when running on an RDD of type T.
sample(withReplacement, fraction, seed) |	Sample a fraction fraction of the data, with or without replacement, using a given random number generator seed.
union(otherDataset)	| Return a new dataset that contains the union of the elements in the source dataset and the argument.
intersection(otherDataset) |	Return a new RDD that contains the intersection of elements in the source dataset and the argument.
distinct([numTasks]))	| Return a new dataset that contains the distinct elements of the source dataset.
groupByKey([numTasks]) |  When called on a dataset of (K, V) pairs, returns a dataset of (K, Iterable<V>) pairs.  Note: If you are grouping in order to perform an aggregation (such as a sum or average) over each key, using reduceByKey or aggregateByKey will yield much better performance. Note: By default, the level of parallelism in the output depends on the number of partitions of the parent RDD. You can pass an optional numTasks argument to set a different number of tasks.
reduceByKey(func, [numTasks]) | 	When called on a dataset of (K, V) pairs, returns a dataset of (K, V) pairs where the values for each key are aggregated using the given reduce function func, which must be of type (V,V) => V. Like in groupByKey, the number of reduce tasks is configurable through an optional second argument.
aggregateByKey(zeroValue)(seqOp, combOp, [numTasks]) |	When called on a dataset of (K, V) pairs, returns a dataset of (K, U) pairs where the values for each key are aggregated using the given combine functions and a neutral "zero" value. Allows an aggregated value type that is different than the input value type, while avoiding unnecessary allocations. Like in groupByKey, the number of reduce tasks is configurable through an optional second argument.
sortByKey([ascending], [numTasks]) |	When called on a dataset of (K, V) pairs where K implements Ordered, returns a dataset of (K, V) pairs sorted by keys in ascending or descending order, as specified in the boolean ascending argument.
join(otherDataset, [numTasks])	| When called on datasets of type (K, V) and (K, W), returns a dataset of (K, (V, W)) pairs with all pairs of elements for each key. Outer joins are supported through leftOuterJoin, rightOuterJoin, and fullOuterJoin.
cogroup(otherDataset, [numTasks]) |	When called on datasets of type (K, V) and (K, W), returns a dataset of (K, (Iterable<V>, Iterable<W>)) tuples. This operation is also called groupWith.
cartesian(otherDataset) |	When called on datasets of types T and U, returns a dataset of (T, U) pairs (all pairs of elements).
pipe(command, [envVars]) |	Pipe each partition of the RDD through a shell command, e.g. a Perl or bash script. RDD elements are written to the process's stdin and lines output to its stdout are returned as an RDD of strings.
coalesce(numPartitions)	| Decrease the number of partitions in the RDD to numPartitions. Useful for running operations more efficiently after filtering down a large dataset.
repartition(numPartitions) |	Reshuffle the data in the RDD randomly to create either more or fewer partitions and balance it across them. This always shuffles all data over the network.
repartitionAndSortWithinPartitions(partitioner) |	Repartition the RDD according to the given partitioner and, within each resulting partition, sort records by their keys. This is more efficient than calling repartition and then sorting within each partition because it can push the sorting down into the shuffle machinery.

#### RDD数据操作
Action |	Meaning
//--- | ---
reduce(func) |	Aggregate the elements of the dataset using a function func (which takes two arguments and returns one). The function should be commutative and associative so that it can be computed correctly in parallel.
collect() |	Return all the elements of the dataset as an array at the driver program. This is usually useful after a filter or other operation that returns a sufficiently small subset of the data.
count() |	Return the number of elements in the dataset.
first() |	Return the first element of the dataset (similar to take(1)).
take(n) |	Return an array with the first n elements of the dataset.
takeSample(withReplacement, num, [seed]) |	Return an array with a random sample of num elements of the dataset, with or without replacement, optionally pre-specifying a random number generator seed.
takeOrdered(n, [ordering]) |	Return the first n elements of the RDD using either their natural order or a custom comparator.
saveAsTextFile(path) |	Write the elements of the dataset as a text file (or set of text files) in a given directory in the local filesystem, HDFS or any other Hadoop-supported file system. Spark will call toString on each element to convert it to a line of text in the file.
saveAsSequenceFile(path) (Java and Scala) |	Write the elements of the dataset as a Hadoop SequenceFile in a given path in the local filesystem, HDFS or any other Hadoop-supported file system. This is available on RDDs of key-value pairs that implement Hadoop's Writable interface. In Scala, it is also available on types that are implicitly convertible to Writable (Spark includes conversions for basic types like Int, Double, String, etc).
saveAsObjectFile(path)  (Java and Scala) |	Write the elements of the dataset in a simple format using Java serialization, which can then be loaded using SparkContext.objectFile().
countByKey() |	Only available on RDDs of type (K, V). Returns a hashmap of (K, Int) pairs with the count of each key.
foreach(func) |	Run a function func on each element of the dataset. This is usually done for side effects such as updating an Accumulator or interacting with external storage systems. Note: modifying variables other than Accumulators outside of the foreach() may result in undefined behavior. See Understanding closures for more details.

#### RDD存储等级
Storage Level |	Meaning
//--- | ---
MEMORY_ONLY	Store | RDD as deserialized Java objects in the JVM. If the RDD does not fit in memory, some partitions will not be cached and will be recomputed on the fly each time they're needed. This is the default level.
MEMORY_AND_DISK	| Store RDD as deserialized Java objects in the JVM. If the RDD does not fit in memory, store the partitions that don't fit on disk, and read them from there when they're needed.
MEMORY_ONLY_SER (Java and Scala)	| Store RDD as serialized Java objects (one byte array per partition). This is generally more space-efficient than deserialized objects, especially when using a fast serializer, but more CPU-intensive to read.
MEMORY_AND_DISK_SER (Java and Scala) |	Similar to MEMORY_ONLY_SER, but spill partitions that don't fit in memory to disk instead of recomputing them on the fly each time they're needed.
DISK_ONLY |	Store the RDD partitions only on disk.
MEMORY_ONLY_2, MEMORY_AND_DISK_2, etc. |	Same as the levels above, but replicate each partition on two cluster nodes.
OFF_HEAP | (experimental)	Similar to MEMORY_ONLY_SER, but store the data in off-heap memory. This requires off-heap memory to be enabled.

#### RDD共享变量

- Broadcast Variables。广播变量可以让每一个机器都缓存一份只读的变量在每一个机器上，而不是将变量copy到每一个机器上。当缓存的变量数据非常大的时候，通过共享变量算法，大大减少了数据传输的消费。

- Accumulators。因为程序是在每一个机器独立执行的，所以没法保存一个全局变量。可以通过累加器（添加、获取数据）来实现计数器的功能。

### Stage DAG有向无环图
spark提交job之后会把job分成多个stage，多个stage之间是有依赖关系的，正如前面所看到的的，stage0依赖于stage1，因此构成了有向无环图DAG。而且stage中的依赖分为窄依赖和宽依赖，窄依赖是一个worker节点的数据只能被一个worker使用，而宽依赖是一个worker节点的数据被多个worker使用。一般讲多个窄依赖归类为一个stage，方便与pipeline管道执行，而将以宽依赖分为一个stage。  

spark正式使用dag有向无环图来实现容错机制，当节点的数据丢失的时候，我们可以通过dag向父节点重新计算数据，这种容错机制成为lineage。  

![spark core](https://raw.githubusercontent.com/mumuhadoop/mumu-spark/master/docs/images/core/spark-core.png) 

## spark sql

### parquet存储格式
parquet是一款列式存储文件格式，parquet专为大数据而生的。当我们要存储的数据列非常多，而业务需求只是需要几列数据的时候，这时候parquet将能发挥重大作用，因为parquet是按照列的格式来存储数据（将相同的列的数据保存在一起），获取数据的时候可以仅仅加载需要列的数据，而不是将所有的列数据全部加载出来，大大较少了无用数据的负载。  
parquet存储优点：
- 列式存储，减少无用数据的加载。
- 相同的列具有相同的数据类型，方便数据进行压缩处理。

## spark streaming

## spark mlib

## spark graphx

## 相关阅读

[hadoop官网文档](http://hadoop.apache.org)

[Apache spark 官网](http://spark.apache.org/)

[spark 过往记忆](https://www.iteblog.com/archives/category/spark/)

## 联系方式

以上观点纯属个人看法，如有不同，欢迎指正。

email:<babymm@aliyun.com>

github:[https://github.com/babymm](https://github.com/babymm)

