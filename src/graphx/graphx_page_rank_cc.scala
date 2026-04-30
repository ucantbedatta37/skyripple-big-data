import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.graphx.{Edge, Graph, VertexId}

/**
 * GraphX airport delay propagation graph:
 * - Vertices: airports (IATA codes)
 * - Directed weighted edges: origin -> destination with weight = sum(delay_minutes)
 * - Outputs:
 *   1) top PageRank airports (hubs)
 *   2) connected components (structural groups)
 *
 * This is intentionally a small MVP-friendly script (run on Parquet produced by the streaming ETL).
 */
object GraphXAirportDelay {
  def main(args: Array[String]): Unit = {
    val inputParquet = sys.env.getOrElse("GRAPHX_INPUT_PARQUET", "./hdfs_like/curated_parquet")
    val outputDir = sys.env.getOrElse("GRAPHX_OUTPUT_DIR", "./hdfs_like/graphx_results")
    val topK = sys.env.getOrElse("GRAPHX_TOP_K", "20").toInt
    val pagerankIters = sys.env.getOrElse("GRAPHX_PAGERANK_ITERS", "10").toInt
    val ccUndirected = sys.env.getOrElse("GRAPHX_CC_UNDIRECTED", "true").toBoolean

    val spark = SparkSession.builder().appName("bdgp-graphx-page-rank-cc").getOrCreate()
    import spark.implicits._

    // Read curated Parquet produced by streaming ETL.
    val df = spark.read.parquet(inputParquet)

    // Weighted directed edges: (origin -> destination) aggregated by sum(delay_minutes).
    val edgesAgg = df
      .select(col("origin"), col("destination"), col("delay_minutes"))
      .where(col("origin").isNotNull && col("destination").isNotNull && col("delay_minutes").isNotNull)
      .groupBy(col("origin"), col("destination"))
      .agg(sum(col("delay_minutes")).alias("weight"))

    val airports = edgesAgg
      .select(col("origin").alias("airport"))
      .union(edgesAgg.select(col("destination").alias("airport")))
      .distinct()
      .collect()
      .map(r => r.getString(0))
      .toSeq

    if (airports.isEmpty) {
      // Write empty outputs so downstream scripts don't crash.
      val emptySchema = Seq("airport", "pagerank")
      spark.createDataFrame(spark.sparkContext.emptyRDD[(String, Double)]).toDF()
      return
    }

    val airportToId: Map[String, VertexId] = airports.zipWithIndex.map { case (a, idx) => (a, idx.toLong) }.toMap
    val idToAirport: Map[VertexId, String] = airportToId.map { case (k, v) => (v, k) }
    val bcIdToAirport = spark.sparkContext.broadcast(idToAirport)

    // Vertices: airportId -> airportCode
    val vertices: org.apache.spark.rdd.RDD[(VertexId, String)] =
      spark.sparkContext.parallelize(idToAirport.toSeq)

    // Directed edges: srcId -> dstId with weight.
    val edges: org.apache.spark.rdd.RDD[Edge[Double]] = edgesAgg.rdd.map { row =>
      val o = row.getString(0)
      val d = row.getString(1)
      val w = row.getLong(2).toDouble
      Edge(airportToId(o), airportToId(d), w)
    }

    val graph = Graph(vertices, edges)

    // Weighted PageRank = hub ranking.
    val pr = graph.staticPageRank(numIter = pagerankIters).vertices
    val prOut = pr.map { case (id, rank) => (bcIdToAirport.value(id), rank) }.toDF("airport", "pagerank")
    val prTop = prOut.orderBy(desc("pagerank")).limit(topK)

    // Connected Components on undirected form (GraphX treats connectivity as undirected).
    val ccEdges =
      if (ccUndirected) {
        graph.edges.flatMap(e => Seq(Edge(e.srcId, e.dstId, e.attr), Edge(e.dstId, e.srcId, e.attr)))
      } else {
        graph.edges
      }

    val cc = Graph(vertices, ccEdges).connectedComponents().vertices
    val ccOut = cc
      .map { case (id, compId) => (bcIdToAirport.value(id), compId) }
      .toDF("airport", "component_id")

    val componentSizes = ccOut.groupBy(col("component_id")).agg(count(lit(1)).alias("size"))

    import java.nio.file.{Files, Paths}
    Files.createDirectories(Paths.get(outputDir))

    prTop.write.mode("overwrite").parquet(outputDir + "/pagerank_top.parquet")
    ccOut.write.mode("overwrite").parquet(outputDir + "/connected_components.parquet")
    componentSizes.write.mode("overwrite").parquet(outputDir + "/component_sizes.parquet")

    println(s"[graphx] airports=${airports.size} edges=${edgesAgg.count()} topK=$topK pagerankIters=$pagerankIters")
    spark.stop()
  }
}

// When this file is loaded via `spark-shell -i`, we need to explicitly run the job.
GraphXAirportDelay.main(Array.empty)
sys.exit(0)

