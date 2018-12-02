import java.io.{File, PrintWriter}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object HybridModelRMSE {
  def main(args: Array[String]): Unit = {

    val start_time = System.currentTimeMillis()

    val test_data_csv_path = "/Users/rishabkumar/Downloads/final results/PittsBurgh/pittsburgh_review_with_text_20_res_lemma_data_train 2.txt"

    val user_pred_csv_path = "/Users/rishabkumar/Downloads/final results/PittsBurgh/Pittsburgh_UserBased_train_predictions.txt"
    val item_pred_csv_path = "/Users/rishabkumar/Downloads/final results/PittsBurgh/Pittsburgh_ItemBased_train_predictions.txt"

    val als_pred_csv_path = "/Users/rishabkumar/Downloads/final results/PittsBurgh/Lakshya_ModelBased_train_predictions_pittsburgh.txt"
    val sgd_pred_csv_path = "/Users/rishabkumar/Downloads/final results/PittsBurgh/SGDBasedPredictions_train_pittusburgh.txt"

    val review_based_csv_path = "/Users/rishabkumar/Downloads/final results/PittsBurgh/TrainTFIDF_ReviewBasedPredictions_pittsburgh.txt"
    val text_based_csv_path = "/Users/rishabkumar/Downloads/final results/PittsBurgh/TrainDoc2Vec_TextBasedPredictions_pittsburgh.txt"
    val category_based_csv_path = "/Users/rishabkumar/Downloads/final results/PittsBurgh/TrainTFIDF_CategoryBasedPredictions_pittsburgh.txt"

    val conf = new SparkConf().setAppName("CalcAvg").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    val test_data = sc.textFile(test_data_csv_path)
    val item_based_pred = sc.textFile(user_pred_csv_path)
    val user_based_pred = sc.textFile(item_pred_csv_path)
    val als_based_pred = sc.textFile(als_pred_csv_path)
    val sgd_based_pred = sc.textFile(sgd_pred_csv_path)
    val review_based_pred = sc.textFile(review_based_csv_path)
    val text_based_pred = sc.textFile(text_based_csv_path)
    val category_based_pred = sc.textFile(category_based_csv_path)

    var test_data_rdd = test_data
      .map(_.split(","))
      .map(item => ((item(0), item(1)), item(2).toDouble))

    var item_based_rdd = item_based_pred
      .map(_.split(","))
      .map(item => ((item(0), item(1)), item(2).toDouble))

    var user_based_rdd = user_based_pred
      .map(_.split(","))
      .map(item => ((item(0), item(1)), item(2).toDouble))

    var als_based_rdd = als_based_pred
      .map(_.split(","))
      .map(item => ((item(0), item(1)), item(2).toDouble))

    var sgd_based_rdd = sgd_based_pred
      .map(_.split(","))
      .map(item => ((item(0), item(1)), item(2).toDouble))

    var review_based_rdd = review_based_pred
      .map(_.split(","))
      .map(item => ((item(0), item(1)), item(2).toDouble))

    var text_based_rdd = text_based_pred
      .map(_.split(","))
      .map(item => ((item(0), item(1)), item(2).toDouble))

    var category_based_rdd = category_based_pred
      .map(_.split(","))
      .map(item => ((item(0), item(1)), item(2).toDouble))

    // category->review->text->sgd->als->user->item
    var combined_pred = item_based_rdd.join(user_based_rdd).join(als_based_rdd).join(sgd_based_rdd).join(text_based_rdd).join(review_based_rdd).join(category_based_rdd)
      .map(item => (item._1, item._2._2 * 0.1 + item._2._1._2 * 0.1 + item._2._1._1._2 * 0.1 + item._2._1._1._1._2 * 0.20 + item._2._1._1._1._1._2 * 0.20 + item._2._1._1._1._1._1._2 * 0.15 + item._2._1._1._1._1._1._1 * 0.15))

    val outFileName = "/Users/rishabkumar/Desktop/DM/final_project/src/main/NewPred/weighted_pred.txt"
    val pwrite = new PrintWriter(new File(outFileName))
    var output_list = combined_pred.collect()
    for (i <- output_list) {
      pwrite.write(i._1._1 + "," + i._1._2 + "," + i._2 + "\n")
    }
    pwrite.close()

    var combined_pred_final = combined_pred.join(test_data_rdd)

    val MSE = combined_pred_final.map { case ((user, product), (r1, r2)) =>
      val err = r1 - r2
      err * err
    }.mean()

    val RMSE = math.sqrt(MSE)
    print("RMSE : " + RMSE)

  }
}
