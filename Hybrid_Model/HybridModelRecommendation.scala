import java.io.{File, PrintWriter}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import scala.collection.mutable.ListBuffer

object HybridModelRecommendation {
  def main(args: Array[String]): Unit = {

    val start_time = System.currentTimeMillis()

    val conf = new SparkConf().setAppName("FinalRecommend").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val top_k = 300

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    val final_predictions_file = args(0)
    val writer_file = "Final_Recommendation_Results.txt"
    val pwriter = new PrintWriter(new File(writer_file))

    var review_preds_path = args(1)
    var text_preds_path = args(2)
    var category_preds_path = args(3)

    var review_preds = sc.textFile(review_preds_path)
    var text_preds = sc.textFile(text_preds_path)
    var category_preds = sc.textFile(category_preds_path)

    var review_business_map = review_preds
      .map(_.split(","))
      .map(item => (item(0), item.drop(1)))
      .mapValues(item => makeTuple(item))
      .mapValues(item => item.take(top_k))
      .mapValues(item => item.map(_._1).toSet)
      .partitionBy(new HashPartitioner(500))
      .cache()


    var text_business_map = text_preds
      .map(_.split(","))
      .map(item => (item(0), item.drop(1)))
      .mapValues(item => makeTuple(item))
      .mapValues(item => item.take(top_k))
      .mapValues(item => item.map(_._1).toSet)
      .partitionBy(new HashPartitioner(500))
      .cache()


    var category_business_map = category_preds
      .map(_.split(","))
      .map(item => (item(0), item.drop(1)))
      .mapValues(item => makeTuple(item))
      .mapValues(item => item.take(top_k))
      .mapValues(item => item.map(_._1).toSet)
      .partitionBy(new HashPartitioner(500))
      .cache()


    var common_preds = review_business_map
      .join(category_business_map)
      .join(review_business_map)
      .mapValues(item => item._1._1.intersect(item._1._2).intersect(item._2))
      .cache()

    val testing_rdd = sc.textFile(final_predictions_file)
      .map(item => item.split(","))
      .map(row => (row(0), row(1)))
      .groupByKey()
      .map(item => (item._1, item._2.toSet))
      .cache()

    val testing_rdd_rating = sc.textFile(final_predictions_file)
      .map(item => item.split(","))
      .map(row => ((row(0), row(1)), row(2).toDouble))
      .cache()

    val test_map = common_preds
      .join(testing_rdd)
      .mapValues(item => item._1.intersect(item._2))
      .cache()

    val test_write = common_preds.collectAsMap()
    for ((k, v) <- test_write) {
      pwriter.write(k + ",")
      for (vals <- v) {
        pwriter.write(vals + ",")
      }
      pwriter.write("\n")
    }

    val total_hits = test_map
      .map(item => (item._1, item._2.size))
      .map(item => item._2)
      .sum

    val total_recommendations = common_preds
      .map(item => (item._1, item._2.size))
      .map(item => item._2)
      .sum

    val precision: Double = total_hits / total_recommendations
    println("Hits are: " + total_hits)
    println("Total Recommendations are is: " + total_recommendations)
    println("Precision is: " + precision)

    pwriter.close()
    println("Time taken is " + (System.currentTimeMillis() - start_time) / 1000 + " secs")

  }

  def makeTuple(strings: Array[String]): List[(String, Double)] = {

    var store: ListBuffer[(String, Double)] = ListBuffer()
    for (item <- strings) {
      val parts = item.split(" ")
      val b_id = parts(0)
      val sim = parts(1).toDouble
      store += ((b_id, sim))
    }
    store.toList
  }


}
