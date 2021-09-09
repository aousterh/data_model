/* EndtoEnd.scala */
import java.io.File
import java.sql.Timestamp
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{avg, col, lit, sum, to_timestamp}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import scala.sys.process._
import scala.util.Try

object EndtoEnd {
  val PARQUET_PATH = "/zq-sample-data/parquet/"
  val RESULTS_PATH = "results"
  val INSTANCE = "m5.xlarge"

  def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess

  def customSelect(df: DataFrame, all_columns: Set[String]) = {
    all_columns.toList.map(column => column match {
      case column if df.columns.contains(column) => col(column)
      case _ => lit(null).as(column)
    })
  }

  abstract class Query
  {
    def run(spark: SparkSession, files: List[String], args: Array[String]) : List[DataFrame]
    def get_validation(result_dfs: List[DataFrame]) : Long
  }

  class SearchQuery extends Query
  {
    def run(spark: SparkSession, files: List[String], args: Array[String]) : List[DataFrame] = {
      val ip = args(0)

      // load dataframes that contain the search column
      val dfs = for {
        x <- files
        val df = spark.read.parquet(x)
        if hasColumn(df, "id.orig_h")
      } yield df

      // issue the query
      val search_results = dfs.map(df => df.filter(col("id.orig_h") === ip))

      return search_results
    }

    def get_validation(result_dfs: List[DataFrame]) : Long = {
      // count records returned
      return result_dfs.map(df => df.count()).sum
    }

    override def toString() : String = {
      return "search id.orig_h"
    }
  }

  class SearchSortHeadQuery extends Query
  {
    def run(spark: SparkSession, files: List[String], args: Array[String]) : List[DataFrame] = {
      val ip = args(0)

      // load dataframes that contain the search column
      val dfs = for {
        x <- files
        val df = spark.read.parquet(x).withColumn("ts", to_timestamp(col("ts")))
        if hasColumn(df, "id.orig_h")
      } yield df

      // rename path fields for smb types because union can't handle the mismatched
      // types of string (smb_files and smb_mapping) and array of strings (smtp)
      val cleaned_dfs = for (df <- dfs) yield {
        if (df.columns.contains("path") && df.schema("path").dataType == StringType)
          df.withColumnRenamed("path", "path_smb")
        else
          df
      }

      // filter dataframes for rows with the matching IP, skip dataframes with
      // no matching rows
      val search_dfs = for {
        df <- cleaned_dfs
        val df_filtered = df.filter(col("id.orig_h") === ip)
        if df_filtered.count() > 0
      } yield df_filtered

      if (search_dfs.length == 1) {
        // only one dataframe had matching results, no uber necessary
        // just sort and return first 1000
        val search_df = search_dfs(0).orderBy("ts").limit(1000).toDF()
        return List(search_df)
      }

      // multiple dataframes match, use an uber schema to combine them
      val all_columns = search_dfs.map(df => df.columns.toSet).reduce(_ ++ _)

      val search_df = search_dfs.map(df => df.select(customSelect(df, all_columns):_*))
        .reduce(_.union(_))
        .orderBy("ts")
        .limit(1000)
        .toDF()

      return List(search_df)
    }

    def get_validation(result_dfs: List[DataFrame]) : Long = {
      // return source port of last result
      val ports = result_dfs(0).select(col("id.orig_p"))
      val count = ports.count()
      val p = ports.collect()(count.toInt-1)(0).asInstanceOf[Long]
      return p
    }

    override def toString() : String = {
      return "search sort head id.orig_h"
    }
  }

  class AnalyticsSumOrigBytesQuery extends Query
  {
    def run(spark: SparkSession, files: List[String], args: Array[String]) : List[DataFrame] = {
      // load dataframes that contain the aggregation column, cast ts column as a timestamp
      val dfs = for {
        x <- files
        val df = spark.read.parquet(x).withColumn("ts", to_timestamp(col("ts")))
        if hasColumn(df, "orig_bytes")
      } yield df

      // convert the timestamps to SQL TIMESTAMP format
      val ts_min = Timestamp.valueOf(args(0).replace("T", " ").replace("Z", ""))
      val ts_max = Timestamp.valueOf(args(1).replace("T", " ").replace("Z", ""))

      // issue the query
      val analytics_df = dfs.map(df => df.filter(col("ts") >= ts_min && col("ts") < ts_max)
        .select(col("orig_bytes"))
        .agg(sum("orig_bytes").as("sum")))
        .reduce(_.union(_))
        .agg(sum("sum").as("sum"))

      return List(analytics_df)
    }

    def get_validation(result_dfs: List[DataFrame]) : Long = {
      return result_dfs(0).collect()(0)(0).asInstanceOf[Long]
    }

    override def toString() : String = {
      return "analytics range ts sum orig_bytes"
    }
  }

  class AnalyticsAvgQuery extends Query
  {
    def run(spark: SparkSession, files: List[String], args: Array[String]) : List[DataFrame] = {
      val agg_column = args(0)

      // load dataframes that contain the aggregation column
      val dfs = for {
        x <- files
        val df = spark.read.parquet(x)
        if hasColumn(df, agg_column)
      } yield df

      // issue the query
      // refer to nested id fields without the "id." since the select removes it
      val analytics_df = dfs.map(df => df.select(col(agg_column)))
        .reduce(_.union(_))
        .agg(avg(agg_column.stripPrefix("id.")).as("avg"))

      return List(analytics_df)
    }

    def get_validation(result_dfs: List[DataFrame]) : Long = {
      // round to Long for type consistency
      return result_dfs(0).collect()(0)(0).asInstanceOf[Double].toLong
    }

    override def toString() : String = {
      return "analytics avg field"
    }
  }

  // array of (trace_file, query class) tuples
  val workloads = Array(("../../workload/trace/network_log_search_needles_30.ndjson", new SearchQuery()),
    //("../../workload/trace/network_log_analytics_30.ndjson", new AnalyticsSumOrigBytesQuery())
    ("../../workload/trace/network_log_search_needles_30.ndjson", new SearchSortHeadQuery()),
    ("../../workload/trace/network_log_analytics_avg_30.ndjson", new AnalyticsAvgQuery()))

  def getListOfParquetFiles(dir: String):List[String] = {
    val d = new File(dir)
    var l = List[File]()
    if (d.exists && d.isDirectory) {
       l = d.listFiles.filter(_.isFile).toList
    }
    l.map(f => f.toString()).filter(_.endsWith(".parquet"))
  }

  def run_benchmark(spark: SparkSession, files: List[String], trace_file_name: String,
    query: Query, include_header: String) = {
    import spark.implicits._

    // read in queries to execute
    val queries = spark.read.json(trace_file_name).select(col("query").as("query"),
      col("arguments").getItem(0).as("arg0"),
      col("arguments").getItem(1).as("arg1")).collect()

    // seq for storing results
    var all_results : Seq[(Int, String, String, String, String, Double, Double,
      Double, Double, String, Long, String)] = Seq()
    var results_fields = Seq("index", "system", "in_format", "out_format",
      "query", "start_time", "real", "user", "sys", "argument_0", "validation",
      "instance")

    println("Starting benchmark")
    var start_time = System.nanoTime
    var index = 0
    for (query_description <- queries) {
      val seq = query_description.toSeq
      val arg0 = if (seq(1) != null) seq(1).toString else ""
      val arg1 = if (seq(2) != null) seq(2).toString else ""

      // run the query and collect results at the driver
      val before = System.nanoTime
      val result_dataframes = query.run(spark, files, Array(arg0, arg1))
      result_dataframes.map(df => df.collect())
      val runtime = (System.nanoTime - before) / 1e9d

      val validation = query.get_validation(result_dataframes)

      all_results = all_results :+ (index, "spark", "parquet", "dataframe",
        query.toString(), (before - start_time) / 1e9d, runtime, 0.0, 0.0, arg0,
        validation, INSTANCE)
      index += 1
      print(".")
    }

    all_results.toDF(results_fields:_*).coalesce(1).write.format("csv")
      .mode("append").option("header", include_header).save(RESULTS_PATH)

    print("\n")
  }

  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Spark Benchmark").getOrCreate()

    val parquet_files = getListOfParquetFiles(PARQUET_PATH)

    var include_header = "true"
    for (w <- workloads) {
      run_benchmark(spark, parquet_files, w._1, w._2, include_header)
      include_header = "false"
    }

    spark.stop()
  }
}
