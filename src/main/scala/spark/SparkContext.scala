package spark
import scala.collection.mutable.ArrayBuffer
import spark.{RDD,parallelizeRDD,localScheduler}

class SparkContext(){
  val scheduler = new localScheduler()
  def parallelize(seq:Seq[Int],nbsplit:Int):parallelizeRDD={
    val interval = seq.length/nbsplit
    var splits = new Array[Seq[Int]](nbsplit)
    for(index <- 0 until nbsplit){
      splits(index) = seq.slice(index*interval, index*interval+interval)
    }
    new parallelizeRDD(this,splits.toSeq)
    
  }
  
  def runJob(rdd:RDD, func:Seq[Int]=>Seq[Int]):Seq[Seq[Int]]={
    val partitions = 0 until rdd.splits.length toArray
    val result = scheduler.runJob(rdd, func, partitions)
    result
  }
}
