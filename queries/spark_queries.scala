import java.io.File
import org.apache.spark.sql.DataFrame

val NDJSON_PATH = "../../zq-sample-data/zeek-ndjson/"

def getListOfFiles(dir: String):List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
       d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
}

// read each JSON file into a dataframe
//val files = getListOfFiles(NDJSON_PATH)

// for now, only use a subset of the data, since some of the data causes errors
// (e.g., some of the data that contains arrays causes problems in the customSelect below)
val files = Array("../../zq-sample-data/zeek-ndjson/conn.ndjson",
    "../../zq-sample-data/zeek-ndjson/http.ndjson",
    "../../zq-sample-data/zeek-ndjson/files.ndjson",
    "../../zq-sample-data/zeek-ndjson/dns.ndjson",
    "../../zq-sample-data/zeek-ndjson/ssl.ndjson",
    /* skip x509 due to lots of dots in names */
    "../../zq-sample-data/zeek-ndjson/weird.ndjson",
    "../../zq-sample-data/zeek-ndjson/syslog.ndjson",
    "../../zq-sample-data/zeek-ndjson/rdp.ndjson",
    "../../zq-sample-data/zeek-ndjson/ntp.ndjson",
    "../../zq-sample-data/zeek-ndjson/smtp.ndjson")
val dfs = for (x <- files) yield spark.read.json(x.toString())

// print out schemas just to check that we've read the data in correctly
dfs.foreach(d => d.printSchema())


println("Analytics query")
println("count total number of records with each distinct source IP")

// first filter to only the dataframes that contain the relevant column
val matching_dfs = for {
    df <- dfs
    if df.columns.contains("id.orig_h")
} yield df

// then issue the query over the matching dataframes
val analytics_df = matching_dfs.map(df => df.select("`id.orig_h`")).reduce(_.union(_)).groupBy("`id.orig_h`").count()
analytics_df.show()


println("Search query")
println("find all records with IP 10.128.0.19, sort by timestamp, and return the first 5")

// rename the column names with dots in them (e.g., "id.orig_h"), because otherwise select throws errors
// TODO: handle this more elegantly?
val renamed_dfs = for {
    df <- dfs
} yield df.withColumnRenamed("id.orig_h", "id_orig_h")
  .withColumnRenamed("id.orig_p", "id_orig_p")
  .withColumnRenamed("id.resp_h", "id_resp_h")
  .withColumnRenamed("id.resp_p", "id_resp_p")

// create the uber schema
val all_columns = renamed_dfs.map(df => df.columns.toSet).reduce(_ ++ _)

def customSelect(df: DataFrame) = {
    all_columns.toList.map(column => column match {
        case column if df.columns.contains(column) => col(column)
        case _ => lit(null).as(column)
    })
}

val search_df = renamed_dfs.map(df => df.select(customSelect(df):_*)
    .filter(col("id_orig_h") === "10.128.0.19"))
    .reduce(_.union(_))
    .orderBy("ts")
    .limit(5)
    .toDF()
search_df.show()


println("Data discovery query")
println("count the number of records with each different schema")

// this works, returns an array of (StructType, Long) tuples
dfs.map(df => (df.schema, df.count))

// this does not work, throws an error, because you can't represent
// schemas within data frames
try {
    dfs.map(df => (df.schema, df.count)).toSeq.toDF("schema", "count")
} catch {
    case exception : MatchError => println("Got an exception: " + exception)
}
