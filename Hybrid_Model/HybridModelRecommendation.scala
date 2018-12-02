import java.io.{File, PrintWriter}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}

object HybridModelRecommendation{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ModelBasedCF").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // Setting Log level to Error
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    val recommendations_file = "/Users/rishabkumar/Desktop/DM/final_project/src/main/NewPred/FinalRecommendations.txt"
    val test_file = "/Users/rishabkumar/Desktop/DM/final_project/src/main/NewPred/las_vegas_review_with_text_50_lemma_test.txt"
    val writer_file = "/Users/rishabkumar/Desktop/DM/final_project/src/main/NewPred/las_vegas_recommendation_results.txt"
    val pwriter = new PrintWriter(new File(writer_file))

    val recommendation_map = sc.textFile(recommendations_file)
      .map(item => item.split(","))
      .map(item => (item(0), item.drop(1).toSet))
      .collectAsMap()


    def getCommonCount(user_id: String, act_business_ids: Set[String]): (Int, Int ,Int) ={
      val common_businesses = act_business_ids.intersect(recommendation_map(user_id))
      (common_businesses.size , act_business_ids.size, recommendation_map(user_id).size)
    }

    val testing_rdd = sc.textFile(test_file)
      .map(item => item.split(","))
      .map(row => (row(0), row(1)))
      .groupByKey()
      .map(item => (item._1, item._2.toSet))
      .cache()

    val test_map = testing_rdd
        .map(item => (item._1, getCommonCount(item._1, item._2)))
        .cache()

    test_map
      .collect()
      .foreach(item => pwriter.write(item._1 +","+item._2._1+","+item._2._3+"\n"))

    val precision_num = test_map
        .map(item => item._2._1 * 1.0 / item._2._3 )
        .sum()

    val total_hits = test_map
        .map(item => item._2._1)
        .sum()

    val total_recommendations = test_map
        .map(item => item._2._3)
        .sum()

    val total_test = test_map
      .map(item => item._2._2)
      .sum()

    println("total_test = ", total_test)
    println("total recommendations = "+ total_recommendations)
    println("total hits = " + total_hits)
    println("precision = "+ precision_num / test_map.collect().length)

    pwriter.close()

  }
}