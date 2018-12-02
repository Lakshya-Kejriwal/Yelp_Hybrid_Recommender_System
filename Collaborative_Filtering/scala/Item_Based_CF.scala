
import java.io.{File, PrintWriter}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD

object Item_Based_CF {
  var conf: SparkConf = null
  var sc: SparkContext = null
  var train_data: RDD[Array[String]] = null
  var test_data: RDD[((String, String), Double)] = null
  var all_review_rdd: RDD[(String)] = null
  var sim_new_map: scala.collection.Map[String, Map[String, Double]] = Map()
  var city = ""
  var run_mode = 0

  def main(args: Array[String]) {

    //time started to compute the total time taken for the entire code to run
    val t = System.currentTimeMillis()

    init_spark()

    process_data(args(0), args(1))

    city = args(2)
    if (args.length == 4)
      run_mode = args(3).toInt

    // path of the output file generated
    val outputFile = new PrintWriter(new File(city + "_ItemBased_test_predictions.txt"))

    //total businesses set
    val all_businesses_set = all_review_rdd
      .map(item => item.split(","))
      .map(item => (item(1).toString, 1))
      .reduceByKey(_ + _)
      .map(item => item._1)
      .collect()
      .toSet

    //total users set
    val all_users_set = all_review_rdd
      .map(item => item.split(","))
      .map(item => (item(0).toString, 1))
      .reduceByKey(_ + _)
      .map(item => item._1)
      .collect()
      .toSet

    // computes average rating corresponding to each business
    val business_averages_map = train_data
      .map(item => (item(1), (1, item(2).toDouble)))
      .reduceByKey((i1, i2) => (i1._1 + i2._1, i1._2 + i2._2))
      .map(item => (item._1, (item._2._2 / item._2._1).toDouble))
      .collectAsMap()

    // computes average rating corresponding to each user and creates its map(String, Double)
    val user_averages_map = train_data
      .map(item => (item(0), (1, item(2).toDouble)))
      .reduceByKey((i1, i2) => (i1._1 + i2._1, i1._2 + i2._2))
      .map(item => (item._1, (item._2._2 / item._2._1).toDouble))
      .collectAsMap()

    val test_businesses_set = test_data
      .map(item => (item._1._2, 1))
      .reduceByKey(_ + _)
      .map(item => item._1)
      .collect()
      .toSet

    val test_users_set = test_data
      .map(item => (item._1._1, 1))
      .reduceByKey(_ + _)
      .map(item => item._1)
      .collect()
      .toSet

    println("unique_business_train = " + business_averages_map.size)
    println("unique_users_train = " + user_averages_map.size)

    println("unique_business_test = " + test_businesses_set.size)
    println("unique_users_test = " + test_users_set.size)

    // used to compute the total average prediction of the entire train data
    val users_and_ratings = train_data
      .map(item => (1, item(2).toDouble))
      .reduce((i1, i2) => (i1._1 + i2._1, i1._2 + i2._2))

    val avg_rating = users_and_ratings._2 * 1.0 / users_and_ratings._1

    // Map[Business_Id: Map[User_Id : Prediction]]
    val business_ratings_map = train_data
      .map(item => (item(1), (item(0), item(2).toDouble)))
      .groupByKey()
      .map(item => (item._1, item._2.toMap))
      .collectAsMap()

    val business_ratings_map_averaged = train_data
      .map(item => (item(1), (item(0), item(2).toDouble - business_averages_map(item(1)))))
      .groupByKey()
      .map(item => (item._1, item._2.toMap))
      .collectAsMap()

    // Map[User_Id : Set[Business_Id1, Business_Id2, Business_Id3, ...]]
    val user_clusters_map = train_data
      .map(item => (item(0), item(1)))
      .groupByKey()
      .map(item => (item._1, item._2.toSet))
      .collectAsMap()

    var test_predictions: Map[(String, String), Double] = Map()
    var train_predictions: Map[(String, String), Double] = Map()

    def compute_similarity_map(): Unit = {
      val prints = new PrintWriter(new File(city + "_item_to_item_similarities.txt"))

      var s: Double = 0.00
      for ((k, v) <- business_averages_map) {

        for ((k2, v2) <- business_averages_map) {
          s = findSim_euc(business_ratings_map_averaged(k), business_ratings_map_averaged(k2))
          if (s > 0.00)
            prints.write(k + "," + k2 + "," + s + "\n")
        }

      }
      prints.close()
    }

    def checkIfSimComputed(b: String, b_to_pred: String): Double = {
      if (sim_new_map.contains(b) && sim_new_map(b).contains(b_to_pred))
        return sim_new_map(b)(b_to_pred)
      else if (sim_new_map.contains(b_to_pred) && sim_new_map(b_to_pred).contains(b))
        return sim_new_map(b_to_pred)(b)
      else
        return 0.0f
    }

    def read_computed_similarity_map(): Unit = {
      val business_similarity_file_path = city + "_item_to_item_similarities.txt"
      sim_new_map = sc.textFile(business_similarity_file_path)
        .map(_.split(","))
        .map(item => (item(0), (item(1), item(2).toDouble)))
        .groupByKey()
        .map(item => (item._1, item._2.toMap))
        .collectAsMap()
    }

    // cases to handle when there is a new user or new business or when there are no two business whose similarity is >0.
    def get_averaged_pred(user_id: String, business_id: String): Double = {
      if (user_clusters_map.contains(user_id) && business_ratings_map.contains(business_id))
        (user_averages_map(user_id) + business_averages_map(business_id) + avg_rating) / 3.0
      else if (!user_clusters_map.contains(user_id) && business_ratings_map.contains(business_id))
        (business_averages_map(business_id) + avg_rating) / 2.0
      else if (user_clusters_map.contains(user_id) && !business_ratings_map.contains(business_id))
        (user_averages_map(user_id) + avg_rating) / 2.0
      else
        avg_rating
    }

    def prediction_test(): Unit = {
      val test_data_iterate = test_data.collect()

      // this loop iterates over all the items in the test data to make predictions
      for (item <- test_data_iterate) {
        val user_to_pred = item._1._1
        val business_to_pred = item._1._2
        val ground_truth = item._2
        var num: Double = 0
        var den: Double = 0
        var pred = 0.00
        //        val neighborhood_size = 40
        // if business_id and user_id exists in the train data then enter else call get_averaged_pred
        if (user_clusters_map.contains(user_to_pred) && business_ratings_map.contains(business_to_pred)) {
          val user_set = user_clusters_map(user_to_pred)
          var flag = 0
          //            var sim_store: Map[String, Double] = Map()
          var temp = 0
          for (business <- user_set) {
            val s = checkIfSimComputed(business, business_to_pred)
            if (s > 0.0) {
              temp += 1
              //                  sim_store += (business -> s)
              num += s * (business_ratings_map(business)(user_to_pred) - business_averages_map(business))
              den += s
              flag = 1
            }
          }
          //            if(flag == 1)
          //              {
          //                if(sim_store.size < neighborhood_size)
          //                {
          //                  for((bus, sim) <- sim_store)
          //                  {
          //                    num += sim * (ratings_map(bus)(user_to_pred) - business_averages_map(bus))
          //                    den += sim
          //                  }
          //                }
          //                else
          //                {
          //                  val sorted_sim = sim_store.toSeq.sortBy(_._2).reverse
          //                  for(a <- 0 until neighborhood_size)
          //                    {
          //                      num += sorted_sim(a)._2 * (ratings_map(sorted_sim(a)._1)(user_to_pred) - business_averages_map(business_to_pred))
          ////                      num += sorted_sim(a)._2 * ratings_map(sorted_sim(a)._1)(user_to_pred)
          //                      den += sorted_sim(a)._2
          //                    }
          //                }
          //                pred = business_averages_map(business_to_pred) + num/den
          ////                pred = num/den
          //              }
          if (flag == 1)
            pred = business_averages_map(business_to_pred) + num / den
          else
            pred = get_averaged_pred(user_to_pred, business_to_pred)
        }
        else
          pred = get_averaged_pred(user_to_pred, business_to_pred)
        if (pred > 5.00)
          pred = 5.00
        test_predictions += ((user_to_pred, business_to_pred) -> pred)
      }
    }

    def prediction_train(): Unit = {
      val train_preds = new PrintWriter(new File(city + "_ItemBased_train_predictions.txt"))

      val train_data_iterate = train_data
        .map(item => ((item(0), item(1)), item(2).toDouble))
        .collect()

      // this loop iterates over all the items in the test data to make predictions
      for (item <- train_data_iterate) {
        val user_to_pred = item._1._1
        val business_to_pred = item._1._2
        val ground_truth = item._2
        var num: Double = 0
        var den: Double = 0
        var pred = 0.00
        //        val neighborhood_size = 40
        // if business_id and user_id exists in the train data then enter else call get_averaged_pred
        if (user_clusters_map.contains(user_to_pred) && business_ratings_map.contains(business_to_pred)) {
          val user_set = user_clusters_map(user_to_pred)
          var flag = 0
          //            var sim_store: Map[String, Double] = Map()
          var temp = 0
          for (business <- user_set) {
            val s = checkIfSimComputed(business, business_to_pred)
            if (s > 0.0) {
              temp += 1
              //                  sim_store += (business -> s)
              num += s * (business_ratings_map(business)(user_to_pred) - business_averages_map(business))
              den += s
              flag = 1
            }
          }
          //            if(flag == 1)
          //              {
          //                if(sim_store.size < neighborhood_size)
          //                {
          //                  for((bus, sim) <- sim_store)
          //                  {
          //                    num += sim * (ratings_map(bus)(user_to_pred) - business_averages_map(bus))
          //                    den += sim
          //                  }
          //                }
          //                else
          //                {
          //                  val sorted_sim = sim_store.toSeq.sortBy(_._2).reverse
          //                  for(a <- 0 until neighborhood_size)
          //                    {
          //                      num += sorted_sim(a)._2 * (ratings_map(sorted_sim(a)._1)(user_to_pred) - business_averages_map(business_to_pred))
          ////                      num += sorted_sim(a)._2 * ratings_map(sorted_sim(a)._1)(user_to_pred)
          //                      den += sorted_sim(a)._2
          //                    }
          //                }
          //                pred = business_averages_map(business_to_pred) + num/den
          ////                pred = num/den
          //              }
          if (flag == 1)
            pred = business_averages_map(business_to_pred) + num / den
          else
            pred = get_averaged_pred(user_to_pred, business_to_pred)
        }
        else
          pred = get_averaged_pred(user_to_pred, business_to_pred)
        if (pred > 5.00)
          pred = 5.00
        train_predictions += ((user_to_pred, business_to_pred) -> pred)
        train_preds.write(user_to_pred+ "," + business_to_pred + "," + pred+"\n")
      }

      train_preds.close()
    }

    def prediction_for_all(): Unit = {
      val test_data_iterate = test_data.collect()
      var num: Double = 0.00
      var den: Double = 0.00
      var pred = 0.00

      //        val neighborhood_size = 40
      val out_predictions = new PrintWriter(new File(city + "_ItemBased_all_predictions.txt"))

      for (user <- test_users_set) {

        val user_pred = user
        for (business <- all_businesses_set) {
          val business_pred = business
          if ((!business_ratings_map.contains(business_pred) && !user_clusters_map.contains(user_pred)) || !business_ratings_map.contains(business_pred) || !user_clusters_map.contains(user_pred)) {
            pred = get_averaged_pred(user_pred, business_pred)
          }
          else if (!business_ratings_map(business_pred).contains(user_pred)) {
            if (user_clusters_map.contains(user_pred) && business_ratings_map.contains(business_pred)) {
              val business_set = user_clusters_map(user_pred)
              var flag = 0
              for (business <- business_set) {
                if (sim_new_map.contains(business_pred) && sim_new_map(business_pred).contains(business)) {
                  val s = sim_new_map(business_pred)(business)
                  num += s * (business_ratings_map(business)(user_pred) - business_averages_map(business))
                  den += s
                  flag = 1
                }
              }
              if (flag == 1)
                pred = business_averages_map(business_pred) + num / den
              else
                pred = get_averaged_pred(user_pred, business_pred)
            }
            else
              pred = get_averaged_pred(user_pred, business_pred)
          }

          if (pred >= 0.0) {
            out_predictions.write(user_pred + "," + business_pred + "," + pred + "\n")
          }
        }

      }
      out_predictions.close()
    }

    def compute_RMSE_and_print_results(predictions: Map[(String, String), Double], file_type: String): Unit = {
      val new_predictions = sc.parallelize(predictions.toList)
      val to_write_predictions = new_predictions
        .map(item => (item._1._1, item._1._2, item._2))
        .collect()
        .sortWith(customSort)

      var ratesAndPreds = test_data.join(new_predictions)
      if (file_type.equals("Train")) {
        val train_req = train_data.map(item => ((item(0), item(1)), item(2).toDouble))
        ratesAndPreds = train_req.join(new_predictions)

      }


      val mse = ratesAndPreds.map { case ((user, business), (s1, s2)) =>
        val err = (s1 - s2)
        err * err
      }.mean()
      val RMSE = Math.sqrt(mse)
      println("RMSE = " + RMSE)

      print_ranges(ratesAndPreds)
      for (item <- to_write_predictions) {
        outputFile.write(item._1 + "," + item._2 + "," + item._3 + "\n")
      }
    }

    println("Computing Similarity...")
    compute_similarity_map()

    println("Reading Similarity...")
    read_computed_similarity_map()

    if (run_mode == 1) {
      //      println("Making predictions for all...")
      //      prediction_for_all()

      println("Computing RMSE for Train File")
      prediction_train()
      compute_RMSE_and_print_results(train_predictions, "Train")
    }

    println("Computing RMSE for Test File")
    prediction_test()
    compute_RMSE_and_print_results(test_predictions, "Test")

    outputFile.close()
    println("time taken = " + (System.currentTimeMillis() - t))
  }

  // custom sort function to sort the results to be printed
  def customSort(s1: (String, String, Double), s2: (String, String, Double)): Boolean = {
    if (s1._1 != s2._1)
      s1._1 < s2._1
    else
      s1._2 < s2._2
  }

  // function to compute the similarity of two business without precomputed averages
  def findSim(h1: Map[String, Double], a1: Double, h2: Map[String, Double], a2: Double): Double = {

    var h1_mod = 0.00
    var comb = 0.00

    //    if(h1.keySet.intersect(h2.keySet).isEmpty)
    //      return 0.00

    for ((k, v) <- h1) {
      h1_mod += Math.pow(v - a1, 2)
      if (h2.contains(k))
        comb += ((v - a1) * (h2(k) - a2))
    }
    h1_mod = Math.sqrt(h1_mod)

    var h2_mod = 0.00
    for ((k, v) <- h2) {
      h2_mod += Math.pow(v - a2, 2)
    }
    h2_mod = Math.sqrt(h2_mod)

    if (h1_mod == 0 || h2_mod == 0)
      return 0.00
    comb / (h1_mod * h2_mod)
  }

  def findSim_pearson(h1: Map[String, Double], h2: Map[String, Double]): Double = {

    var h1_mod = 0.00
    var h2_mod = 0.00
    var comb = 0.00

    val common = h1.keySet.intersect(h2.keySet)
    if (common.isEmpty)
      return 0.00

    for (item <- common) {
      comb += h1(item) * h2(item)
    }

    for ((k, v) <- h1) {
      h1_mod += Math.pow(v, 2).toFloat
    }
    h1_mod = Math.sqrt(h1_mod).toFloat

    for ((k, v) <- h2) {
      h2_mod += Math.pow(v, 2).toFloat
    }
    h2_mod = Math.sqrt(h2_mod).toFloat

    if (h1_mod == 0 || h2_mod == 0)
      return 0.00
    comb / (h1_mod * h2_mod)
  }

  def findSim_euc(user_1: Map[String, Double], user_2: Map[String, Double]): Double = {

    val common_users = user_1.keySet.intersect(user_2.keySet)

    // if no keys are in common that return similarity as 0
    if (common_users.isEmpty)
      return 0

    // calculate numerators
    var dot_product: Double = 0
    for (item <- common_users)
      dot_product += math.pow(user_1(item) - user_2(item), 2)

    dot_product = math.sqrt(dot_product)
    val similarity = math.exp(dot_product) / (math.exp(dot_product) + dot_product)

    similarity
  }

  def findSim_man(user_1: Map[String, Double], user_2: Map[String, Double]): Double = {

    val common_users = user_1.keySet.intersect(user_2.keySet)

    // if no keys are in common that return similarity as 0
    if (common_users.isEmpty)
      return 0

    // calculate numerators
    var dot_product: Double = 0
    for (item <- common_users)
      dot_product += math.abs(user_1(item) - user_2(item))

    val similarity = math.exp(dot_product) / (math.exp(dot_product) + dot_product)

    similarity
  }

  def findSim_jaccard(user_1: Map[String, Double], user_2: Map[String, Double]): Double = {

    val common_users = user_1.keySet.intersect(user_2.keySet)

    // if no keys are in common that return similarity as 0
    if (common_users.isEmpty)
      return 0

    // calculate numerators
    var dot_product: Double = 0.0
    var num: Double = 0.0
    var denom: Double = 0.0
    for (item <- common_users) {
      num += math.min(user_1(item), user_2(item))
      denom += math.max(user_1(item), user_2(item))
    }

    val similarity = num / denom
    similarity
  }

  def findSim_tanimoto(user_1: Map[String, Double], user_2: Map[String, Double]): Double = {

    val common_users = user_1.keySet.intersect(user_2.keySet)

    // if no keys are in common that return similarity as 0
    if (common_users.isEmpty)
      return 0

    // calculate numerators
    var dot_product: Double = 0.0
    var num: Double = 0.0
    var denom: Double = 0.0
    for (item <- common_users) {
      dot_product += math.abs(user_1(item) - user_2(item))
    }

    var denominator_item_1: Double = 0
    var denominator_item_2: Double = 0

    for ((k, v) <- user_1)
      denominator_item_1 += v * v
    for ((k, v) <- user_2)
      denominator_item_2 += v * v

    val denominator = denominator_item_1 + denominator_item_2 - dot_product
    if (denominator != 0)
      dot_product / denominator
    else
      0.0
  }

  // function for printing the range of predictions made i.e. ( [0-1), [1-2), [2-3), [3-4), [4-5])
  def print_ranges(ratesAndPreds: RDD[((String, String), (Double, Double))]): Unit = {
    val ranges = ratesAndPreds.map(item => {
      val temp = Math.abs(item._2._1 - item._2._2)
      if (temp >= 4.00) (4, 1)
      else if (temp >= 3.00) (3, 1)
      else if (temp >= 2.00) (2, 1)
      else if (temp >= 1.00) (1, 1)
      else (0, 1)
    }).reduceByKey(_ + _).collect()

    var four_count: Int = 0
    var three_count: Int = 0
    var two_count: Int = 0
    var one_count: Int = 0
    var zero_count: Int = 0
    for (item <- ranges) {
      if (item._1 == 4)
        four_count = item._2
      else if (item._1 == 3)
        three_count = item._2
      else if (item._1 == 2)
        two_count = item._2
      else if (item._1 == 1)
        one_count = item._2
      else
        zero_count = item._2
    }

    println(">=0 and <1: " + zero_count)
    println(">=1 and <2: " + one_count)
    println(">=2 and <3: " + two_count)
    println(">=3 and <4: " + three_count)
    println(">=4: " + four_count)
  }

  def init_spark(): Unit = {
    //initialise spark configuration and start spark session
    conf = new SparkConf().setAppName("ModelBasedCF").setMaster("local[*]")
      .set("spark.executor.memory", "6g")
      .set("spark.driver.memory", "6g")
    sc = new SparkContext(conf)

    // Setting Log level to Error
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
  }

  def process_data(train_path: String, test_path: String): Unit = {
    val train_rdd = sc.textFile(train_path)
    val test_rdd = sc.textFile(test_path)

    // create rdd of the train data and remove the header
    train_data = train_rdd
      .map(item => item.split(","))

    // create rdd of the test data and remove the header
    test_data = test_rdd
      .map(item => item.split(",")).map(item => ((item(0), item(1)), item(2).toDouble)).persist()

    all_review_rdd = test_rdd
      .union(train_rdd)
  }

  def process_data_ramdomly(file_name: String, avg_rating_req: Double): Unit = {

    val pitts_review_data_path = "/Users/dharmik/Desktop/Semester3/Data Mining/final_project/src/main/Dharmik_Data/" + file_name
    all_review_rdd = sc.textFile(pitts_review_data_path)

    println("Unique users = " + all_review_rdd
      .map(item => item.split(","))
      .map(item => (item(0), 1))
      .reduceByKey(_ + _)
      .collect()
      .length)

    println("Unique businesses = " + all_review_rdd
      .map(item => item.split(","))
      .map(item => (item(1), 1))
      .reduceByKey(_ + _)
      .collect()
      .length)

    val splits = all_review_rdd.randomSplit(Array(0.8, 0.2), 11L)
    val train = splits(0)
    val test = splits(1)

    println("Total_data_count = " + all_review_rdd.count())
    println("train_data_count = " + train.count())
    println("test_data_count = " + test.count())

    // create rdd of the train data and remove the header
    train_data = train
      .map(item => item.split(","))

    // create rdd of the test data and remove the header
    test_data = test
      .map(item => item.split(",")).map(item => ((item(0), item(1)), item(2).toDouble)).persist()

  }

}
