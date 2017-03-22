package com.needine.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.clustering._
import org.apache.spark.rdd._

/*
 * 
 *  APP de analisis de trazas de redes capturadas con tshark. Es el primer paso para luego hacer
 *  otra version con streams con Kafka Connect.
 * 
 */
object Demo {

  
  def distance (a: Vector, b: Vector) = math.sqrt(a.toArray.zip(b.toArray).map(p => p._1 -p._2).map(d => d*d).sum)
    
  def distToCentroid(datum: Vector, model: KMeansModel) = {
    val cluster = model.predict(datum)
    val centroid = model.clusterCenters(cluster)
    distance(centroid, datum)
    
  }
    
  def clusteringScore (data: RDD[Vector], k: Int) ={
    val kmeans = new KMeans()
    kmeans.setK(k)
    val model = kmeans.run(data)
    data.map(datum => distToCentroid(datum, model)).mean()
          
  }

  
  //class 
  def IsIP4(s: String): Boolean = {
    
    if (s.contains(":")) 
      return false 
    else 
      return true
  }
  
  def main(args: Array[String]) = {
    
    val conf = new SparkConf()
      .setAppName("Network Analysis V0.1 ")
      .setMaster("local")      
    val sc = new SparkContext(conf)
    
    //val sc = new SparkContext(new SparkConf())
    
    val rawData = sc.textFile("/home/hduser/Documents/ml/networking/KDD99/kddcup.data.corrected")
    
    rawData.take(20).foreach(println)
    println(rawData.count())
    rawData.map(_.split(",").last).countByValue().toSeq.sortBy(_._2).reverse.foreach(println)
    
    val labelsAndData = rawData.map { line =>
      val buffer = line.split(",").toBuffer
      buffer.remove(1, 3)
      val label = buffer.remove(buffer.length - 1)
      val vector = Vectors.dense(buffer.map(_.toDouble).toArray)
      (label, vector)
    }
    val data = labelsAndData.values.cache()
    
    data.take(10).foreach(println)
    
    /*
    val kmeans = new KMeans()
    val model = kmeans.run(data)
    model.clusterCenters.foreach { println }
    
    val clusterLabelCount = labelsAndData.map{ case(label, datum) =>
      val cluster = model.predict(datum)
      (cluster, label)
    }.countByValue
    
    clusterLabelCount.toSeq.sorted.foreach{ case((cluster, label), count) =>
      println(f"$cluster%1s$label%18s$count%8s")
    }
    
    */    
    
    (5 to 35 by 10).par.map{k =>
      (k, clusteringScore(data, k))
    }.foreach(println)
    
    
    /*
     * 
    val traces = rawData.map(_.split(" ")).take(10).foreach{t =>
      println(t(1)) // ID
      println(t(2)) // TIME
      //println(t(3))
      println(t(4)) //ORIGIN
      //println(t(5))
      println(t(6)) // DESTINATION
      println(t(7)) // PROTOCOL
      println("<[*************]>")
    }
     * 
     */
    /*
    val head = rawData.take(100)
    val tracesIP = head.map(_.split(" ")).filter(t =>IsIP4(t(4)) & t(6).contains("TCP"))
    
    tracesIP.foreach{t =>
      
      println("TRACE: " + t(1)) // ID
      println("Time = " + t(2)) // TIME
      println("Origin = " + t(3)) //ORIGIN
      //println(t(4)) 
      println("Destination = " + t(5)) // DESTINATION
      println("Protocol = " + t(6)) // PROTOCOL
      println(t(7)) 
      println(t(8))
      println(t(9))
      println(t(10))
      println(t(11))
      println(t(12))
      
      println(" ")
    }
    */
    
    println("AAA")
    
  }
}