/* EndtoEnd.scala */
import java.io.File
import java.sql.Timestamp
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, lit, sum, to_timestamp}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import scala.sys.process._
import scala.util.Try

object EndtoEnd {
  val PARQUET_PATH = "/zq-sample-data/parquet/"
  val WORKLOAD = "../../workload/trace/network_log_search_30.ndjson"
//  val WORKLOAD = "../../workload/trace/network_log_analytics_30.ndjson"
  val RESULTS_PATH = "results"
  val OUTPUT_PATH = "output"

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

      // write the results out as parquet, to ensure the query actually executed
      for (df <- search_results)
        df.write.mode("append").parquet(OUTPUT_PATH)

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

  class SearchUberQuery extends Query
  {
    def run(spark: SparkSession, files: List[String], args: Array[String]) : List[DataFrame] = {
      val ip = args(0)

      // load dataframes that contain the search column
      val dfs = for {
        x <- files
        val df = spark.read.parquet(x)
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

      // create the uber schema
      val all_columns = cleaned_dfs.map(df => df.columns.toSet).reduce(_ ++ _)

      // issue the query, use customSelect to uber the results as you go
      val search_df = cleaned_dfs.map(df => df.select(customSelect(df, all_columns):_*)
        .filter(col("id.orig_h") === ip))
        .reduce(_.union(_))
        .toDF()

      // write the results out as parquet, to ensure the query actually executed
      search_df.write.mode("append").parquet(OUTPUT_PATH)

      return List(search_df)
    }

    def get_validation(result_dfs: List[DataFrame]) : Long = {
      // count records returned
      return result_dfs.map(df => df.count()).sum
    }

    override def toString() : String = {
      return "search id.orig_h uber"
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
        .agg(sum("orig_bytes")))
        .reduce(_.union(_))
        .agg(sum("sum(orig_bytes)"))
        .select(col("sum(sum(orig_bytes))").as("sum"))

      // write the results out as parquet, to ensure the query actually executed
      analytics_df.write.mode("append").parquet(OUTPUT_PATH)

      return List(analytics_df)
    }

    def get_validation(result_dfs: List[DataFrame]) : Long = {
      return result_dfs(0).collect()(0)(0).asInstanceOf[Long]
    }

    override def toString() : String = {
      return "analytics sum orig_bytes"
    }
  }

  // mapping from string ID of each query to its Query class
  val QUERY_ARRAY = Array(new SearchQuery(), new SearchUberQuery(),
    new AnalyticsSumOrigBytesQuery())
  val QUERY_MAP = QUERY_ARRAY.map(q => q.toString() -> q).toMap

  def getListOfParquetFiles(dir: String):List[String] = {
    val d = new File(dir)
    var l = List[File]()
    if (d.exists && d.isDirectory) {
       l = d.listFiles.filter(_.isFile).toList
    }
    l.map(f => f.toString()).filter(_.endsWith(".parquet"))
  }

  def run_benchmark(spark: SparkSession, files: List[String]) = {
    import spark.implicits._

    // read in queries to execute
    val queries = spark.read.json(WORKLOAD).select(col("query").as("query"),
      col("arguments").getItem(0).as("arg0"),
      col("arguments").getItem(1).as("arg1")).collect()

    // seq for storing results
    var all_results : Seq[(Int, String, String, String, String, Double, Double,
      Double, Double, String, Long)] = Seq()
    var results_fields = Seq("index", "system", "in_format", "out_format",
      "query", "start_time", "real", "user", "sys", "argument_0", "validation")

    println("Starting benchmark")
    var start_time = System.nanoTime
    var index = 0
    for (query_description <- queries) {
      val seq = query_description.toSeq
      val query = QUERY_MAP(seq(0).toString())
      val arg0 = if (seq(1) != null) seq(1).toString else ""
      val arg1 = if (seq(2) != null) seq(2).toString else ""

      val before = System.nanoTime
      val result_dataframes = query.run(spark, files, Array(arg0, arg1))
      val runtime = (System.nanoTime - before) / 1e9d

      val validation = query.get_validation(result_dataframes)

      all_results = all_results :+ (index, "spark", "parquet", "dataframe",
        query.toString(), (before - start_time) / 1e9d, runtime, 0.0, 0.0, arg0,
        validation)
      index += 1
      print(".")
    }

    all_results.toDF(results_fields:_*).coalesce(1).write.format("csv")
      .option("header", "true").save(RESULTS_PATH)
  }

  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Spark Benchmark").getOrCreate()

    val parquet_files = getListOfParquetFiles(PARQUET_PATH)
    run_benchmark(spark, parquet_files)

    spark.stop()
  }
}
