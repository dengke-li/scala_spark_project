package spark

import spark.SparkContext

/**
 * A Resilient Distributed Dataset (RDD), the basic abstraction in Spark. Represents an immutable, 
 * partitioned collection of elements that can be operated on in parallel.
 *
 * Each RDD is characterized by five main properties:
 * - A list of splits (partitions)
 * - A function for computing each split
 * - A list of dependencies on other RDDs
 * - Optionally, a Partitioner for key-value RDDs (e.g. to say that the RDD is hash-partitioned)
 * - Optionally, a list of preferred locations to compute each split on (e.g. block locations for
 *   HDFS)
 *
 * All the scheduling and execution in Spark is done based on these methods, allowing each RDD to 
 * implement its own way of computing itself.
 *
 * This class also contains transformation methods available on all RDDs (e.g. map and filter). In 
 * addition, PairRDDFunctions contains extra methods available on RDDs of key-value pairs, and 
 * SequenceFileRDDFunctions contains extra methods for saving RDDs to Hadoop SequenceFiles.
 */
abstract class RDD(sc: SparkContext){
  def compute(split:Seq[Int]):Seq[Int]
  def splits:Seq[Seq[Int]]
  def map(f:Int=>Int):MappedRDD={
    new MappedRDD(this, sc, f)
  }
  
  def collect():Array[Int]={
    val result = sc.runJob(this,list=>list)
    //Array.concat(result.asInstanceOf[Array[Int]])
    result.flatten.toArray
  }
  
}

class Iterator

// here when extends RDD, should indicate the parameter
class parallelizeRDD(sc:SparkContext, data:Seq[Seq[Int]]) extends RDD(sc){
  override def splits:Seq[Seq[Int]] = {data}
  override def compute(split:Seq[Int]):Seq[Int]={
    split
    }
}

class MappedRDD(prev:RDD,sc:SparkContext,f:Int=>Int) extends RDD(sc) {

  override def splits = prev.splits
  override def compute(split:Seq[Int]):Seq[Int]= {
    prev.compute(split).map(e=>f(e))
    }
}