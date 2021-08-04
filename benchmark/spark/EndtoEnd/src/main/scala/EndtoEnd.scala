/* EndtoEnd.scala */
import java.io.File
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import scala.sys.process._
import scala.util.Try

object EndtoEnd {
  val PARQUET_PATH = "/zq-sample-data/parquet/"
  val WORKLOAD = "../../workload/trace/network_log_search_30.ndjson"
  val RESULTS_PATH = "results"
  val OUTPUT_PATH = "output"
  val UNION_SEARCH = false

  def getListOfParquetFiles(dir: String):List[String] = {
    val d = new File(dir)
    var l = List[File]()
    if (d.exists && d.isDirectory) {
       l = d.listFiles.filter(_.isFile).toList
    }
    l.map(f => f.toString()).filter(_.endsWith(".parquet"))
  }

  def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess

  def customSelect(df: DataFrame, all_columns: Set[String]) = {
    all_columns.toList.map(column => column match {
      case column if df.columns.contains(column) => col(column)
      case _ => lit(null).as(column)
    })
  }

  def search(spark: SparkSession, files: List[String], ip: String) : List[DataFrame] = {
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

  def search_union(spark: SparkSession, files: List[String], ip: String) : List[DataFrame] = {
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

    search_df.write.mode("append").parquet(OUTPUT_PATH)

    return List(search_df)
  }

  def run_benchmark(spark: SparkSession, files: List[String]) = {
    import spark.implicits._

    // read in queries to execute
    val queries = spark.read.json(WORKLOAD).select(
      col("arguments").getItem(0).as("arg0"), col("query").as("query")).collect()

    // seq for storing results
    var all_results : Seq[(Int, String, String, String, String, Double, Double,
      Double, Double, String, Long)] = Seq()
    var results_fields = Seq("index", "system", "in_format", "out_format",
      "query", "start_time", "real", "user", "sys", "argument_0", "validation")

    println("Starting benchmark")
    var start_time = System.nanoTime
    var index = 0
    for (query_description <- queries) {
      val arg0 = query_description.toSeq(0).toString
      val query = query_description.toSeq(1).toString

      val before = System.nanoTime
      var result_dataframes: List[DataFrame] = List()
      if (UNION_SEARCH) {
        result_dataframes = search_union(spark, files, arg0)
      } else {
        result_dataframes = search(spark, files, arg0)
      }
      val runtime = (System.nanoTime - before) / 1e9d

      // count records returned, for validation
      var count: Long = 0
      result_dataframes.foreach(count += _.count())

      all_results = all_results :+ (index, "spark", "parquet", "dataframe",
        query, (before - start_time) / 1e9d, runtime, 0.0, 0.0, arg0, count)
      index += 1
      print(".")
    }

    all_results.toDF(results_fields:_*).coalesce(1).write.format("csv")
      .option("header", "true").save(RESULTS_PATH)
  }

  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()

    val parquet_files = getListOfParquetFiles(PARQUET_PATH)
    run_benchmark(spark, parquet_files)

    spark.stop()
  }
}
