package com.zy.sparkquanwei

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.util
case class Person(name: String, age: Int)
object GraphxTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ConnectedComponentsDemo").setMaster("local[*]")
    val sc = new SparkContext(conf)


    val people = sc.textFile("file:///J:\\test_data\\people.csv")
    // vertices
    val peopleRDD: RDD[(VertexId, Person)] = people.map(line => line.split(","))
      .map(row => (row(0).toLong, Person(row(1), row(2).toInt)))

    val links = sc.textFile("file:///J:\\test_data\\links.csv")
    // edges
    val linksRDD: RDD[Edge[String]] = links.map({
      line =>
        val row = line.split(",")
        Edge(row(0).toInt, row(2).toInt, row(1))
    })

    val peopleGraph: Graph[Person, String] = Graph(peopleRDD, linksRDD)
    // spark graphX已经封装了connectedComponents，直接使用即可
    val cc = peopleGraph.connectedComponents()
    // 与原来的图join，cc代表新图的属性，p_id代表就图的属性(Person)
    val newGraph = cc.outerJoinVertices(peopleRDD)((id, cc_id, p_id)=>(cc_id,p_id.get.name,p_id.get.age))

    cc.vertices.map(_._2).collect.distinct.foreach( // 获取连通分量的id
      id =>{

        // subgraph用于截取满足条件的子图
        val sub = newGraph.subgraph(vpred = (id1, id2) => id2._1==id)
        val vts = sub.vertices.collect().iterator
        var set = new util.HashSet[Long]()
        var str = ""
        while (vts.hasNext){
          val tpl = vts.next()
          set.add(tpl._1.toLong)
          set.add(tpl._2._1)
        }
        val it = set.iterator()
        while (it.hasNext){
          str += it.next() + ","
        }
        println(str)
      })
  }
}
