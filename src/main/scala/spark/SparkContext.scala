package spark
import scala.collection.mutable.ArrayBuffer
import spark.{RDD,parallelizeRDD,localScheduler}

class SparkContext(){
  val scheduler = new localScheduler()
  def parallelize(seq:Seq[Int],nbsplit:Int):parallelizeRDD={
    val interval = seq.length/nbsplit
    var splits = new Array[Split](nbsplit)
    for(index <- 0 until nbsplit){
      splits(index) = new Split(seq.slice(index*interval, index*interval+interval))
    }
    new parallelizeRDD(this,splits)
    
  }
  
  def runJob(rdd:RDD, func:Seq[Int]=>Seq[Int]):Seq[Iterator[Int]]={
    val partitions = 0 until rdd.splits.length toArray
    val result = scheduler.runJob(rdd, func, partitions)
    result
  }
}
