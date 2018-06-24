package spark

import spark.SparkContext
import scala.collection.Iterator

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
abstract class RDD(sc: SparkContext) extends Serializable{
  def compute(split:Split):Iterator[Int]
  def splits:Array[Split]
  def map(f:Int=>Int):MappedRDD={
    new MappedRDD(this, sc, f)
  }
  
  def filter(f:Int=>Boolean):FilterRDD={
    new FilterRDD(this, sc, f)
  }
  
  def collect():Array[Int]={
    val result = sc.runJob(this,list=>list)
    //Array.concat(result.asInstanceOf[Array[Int]])
    result.flatten.toArray
  }
  
}

// here when extends RDD, should indicate the parameter
class parallelizeRDD(sc:SparkContext, data:Array[Split]) extends RDD(sc){
  override def splits:Array[Split] = {data}
  override def compute(split:Split):Iterator[Int]={
    split.getSeq().toIterator
    }
}

class FilterRDD(prev:RDD, sc:SparkContext,f:Int=>Boolean) extends RDD(sc){
  override def splits = prev.splits
  override def compute(split:Split):Iterator[Int]= {
    val temp = prev.compute(split)
    temp.filter(f)
    }
}

class MappedRDD(prev:RDD,sc:SparkContext,f:Int=>Int) extends RDD(sc) {

  override def splits = prev.splits
  override def compute(split:Split):Iterator[Int]= {
    prev.compute(split).map(e=>f(e))
    }
}