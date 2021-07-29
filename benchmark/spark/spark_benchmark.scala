import java.io.File
import org.apache.spark.sql.DataFrame
import scala.collection.mutable.ArrayBuffer
import scala.sys.process._

val PARQUET_PATH = "/zq-sample-data/parquet/"

def getListOfFiles(dir: String):List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
       d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
}

val files = getListOfFiles(PARQUET_PATH)

class Benchmark(description: String) {
      var samples: ArrayBuffer[Double] = new ArrayBuffer[Double]()
      var sample_start: Long = 0

      def start_sample() {
	  sample_start = System.nanoTime
      }

      def end_sample() {
	  samples += ((System.nanoTime - sample_start) / 1e9d)
	  sample_start = 0
      }

      def print_results(warm_up: Int) {
	  print("Results for benchmark " + description + ":\n")
	  print("total samples: " + samples.length + ", warm up samples: " + warm_up + "\n")

	  var total = 0d
	  for (i <- 0 to samples.length - 1) {
	      print(samples(i) + "\n")

	      if (i >= warm_up)
		  total += samples(i)
	  }
	  print("average: " + total / (samples.length - warm_up) + "\n")
      }
}

def bench_analytics(): Benchmark = {
	println("Analytics query")
	println("count total number of records with each distinct source IP")

	val b = new Benchmark("Analytics");

    for (i <- 1 to 15) {
	// for this query, this doesn't seem to make much difference
	"sudo ./flush_cache.sh"!

	b.start_sample()

	val dfs = for (x <- files) yield spark.read.parquet(x.toString())

	// first filter to only the dataframes that contain the relevant column
	val matching_dfs = for {
	    df <- dfs
	    if df.columns.contains("id.orig_h")
	} yield df

	// then issue the query over the matching dataframes
	val analytics_df = matching_dfs.map(df => df.select("`id.orig_h`")).reduce(_.union(_)).groupBy("`id.orig_h`").count().show()

	b.end_sample()
    }

    return b
}

val bench = bench_analytics()
bench.print_results(5)
