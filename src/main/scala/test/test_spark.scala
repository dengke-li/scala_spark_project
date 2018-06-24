package test
import spark.SparkContext

object test_spark {
  def main(arrays:Array[String])={
    val sc = new SparkContext()
    val seq = Seq(10,6,0,4)// Seq is trait
    val rdd1 = sc.parallelize(seq,2)
    val mappedrdd = rdd1.map(x=>x+1)
    val filterrdd = mappedrdd.filter { x => x>5 }
    val result = filterrdd.collect()
    
    result.foreach(e=>println(e))
  }
}