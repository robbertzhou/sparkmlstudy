package com.zy.yuanli

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MapGraphX {

  def main(args: Array[String]): Unit = {
    //设置运行环境
    val conf = new SparkConf().setAppName("SimpleGraphX").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    //设置users顶点
    val users: RDD[(VertexId, (String, Int))] =
      sc.parallelize(Array(
        (1L, ("Alice", 28)),
        (2L, ("Bob", 27)),
        (3L, ("Charlie", 65)),
        (4L, ("David", 42)),
        (5L, ("Ed", 55)),
        (6L, ("Fran", 50))
      ))

    //设置relationships边
    val relationships: RDD[Edge[Int]] =
      sc.parallelize(Array(
        Edge(2L, 1L, 7),
        Edge(2L, 4L, 2),
        Edge(3L, 2L, 4),
        Edge(3L, 6L, 3),
        Edge(4L, 1L, 1),
        Edge(5L, 2L, 2),
        Edge(5L, 3L, 8),
        Edge(5L, 6L, 3)
      ))
    // 定义默认的作者,以防与不存在的作者有relationship边
    val defaultUser = ("John Doe", 0)

    println("（1）通过上面的项点数据和边数据创建图对象")
    // Build the initial Graph
    val graph: Graph[(String, Int), Int] = Graph(users, relationships,
      defaultUser)
    val connect = graph.connectedComponents()
    val rs = connect.vertices.collect()


    //val newVertices = graph.vertices.map { case (id, attr) => (id, mapUdf(id, attr)) }
    //val newGraph = Graph(newVertices, graph.edges)

    graph.vertices.collect.foreach(println(_))

    println("在已有图上新建新的图")
    var graph2: Graph[(String, Int), Int] = graph.mapVertices((vid: VertexId, attr: (String, Int)) => (attr._1, 2 * attr._2))
    graph2.vertices.collect.foreach(println(_))

    println("（2）使用mapEdges函数遍历所有的边，新增加一个属性值然后构建出新的图")
    var graph3: Graph[(String, Int), (Int, Int)] = graph.mapEdges(e => (e
      .attr , 100))
    graph3.edges.collect.foreach(println(_))


    println("（3）使用mapTriplets函数遍历所有的三元组，新增加一个属性值，然后返回新的图")
    var graph4: Graph[(String, Int),(Int, Int)] = graph.mapTriplets(t => (t.attr,
      10))
    graph4.edges.collect.foreach(println(_))

  }

}