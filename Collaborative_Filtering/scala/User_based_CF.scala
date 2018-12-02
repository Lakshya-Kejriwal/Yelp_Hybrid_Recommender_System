import java.io.{File, PrintWriter}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD

object User_based_CF {
  var conf: SparkConf = null
  var sc: SparkContext = null
  var train_data: RDD[Array[String]] = null
  var test_data: RDD[((String, String), Float)] = null
  var all_review_rdd: RDD[(String)] = null
  var user_similarity_map: Map[String, Map[String, Float]] = Map()
  var sim_new_map: scala.collection.Map[String, Map[String, Float]] = Map()
  var city = ""
  var run_mode = 0

  def main(args: Array[String]) {

    //time started to compute the total time taken for the entire code to run
    val t = System.currentTimeMillis()

    // initializing spark
    init_spark()

    // processing data
    process_data(args(0), args(1))

    //total businesses set
    val all_businesses_set = all_review_rdd
      .map(item => item.split(","))
      .map(item => (item(1), 1))
      .reduceByKey(_ + _)
      .map(item => item._1)
      .collect()
      .toSet

    //total users set
    val all_users_set = all_review_rdd
      .map(item => item.split(","))
      .map(item => (item(0), 1))
      .reduceByKey(_ + _)
      .map(item => item._1)
      .collect()
      .toSet

    city = args(2)
    if(args.length == 4)
      run_mode = args(3).toInt

    // path of the output file generated
    val outputFile = new PrintWriter(new File(city + "_UserBased_test_predictions.txt"))

    // computes average rating corresponding to each business and creates its map(String, Float)
    val business_averages_map = train_data
      .map(item => (item(1), (1, item(2).toFloat)))
      .reduceByKey((i1, i2) => (i1._1 + i2._1, i1._2 + i2._2))
      .map(item => (item._1, item._2._2 / item._2._1))
      .collectAsMap()

    // computes average rating corresponding to each user and creates its map(String, Float)
    val user_averages_map = train_data
      .map(item => (item(0), (1, item(2).toFloat)))
      .reduceByKey((i1, i2) => (i1._1 + i2._1, i1._2 + i2._2))
      .map(item => (item._1, item._2._2 / item._2._1))
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
      .map(item => (1, item(2).toFloat))
      .reduce((i1, i2) => (i1._1 + i2._1, i1._2 + i2._2))

    val avg_rating = users_and_ratings._2 * 1.0 / users_and_ratings._1

    // Map[User_Id: Map[Business_Id : Prediction]]
    val user_ratings_map = train_data
      .map(item => (item(0), (item(1), item(2).toFloat)))
      .groupByKey()
      .map(item => (item._1, item._2.toMap))
      .collectAsMap()

    val user_ratings_map_averaged = train_data
      .map(item => (item(0), (item(1), item(2).toFloat - user_averages_map(item(0)))))
      .groupByKey()
      .map(item => (item._1, item._2.toMap))
      .collectAsMap()

    // Map[Business_Id : Set[User_Id1...]]
    val business_clusters_map = train_data
      .map(item => (item(1), item(0)))
      .groupByKey()
      .map(item => (item._1, item._2.toSet))
      .collectAsMap()

    var test_predictions: Map[(String, String), Float] = Map()
    var train_predictions: Map[(String, String), Float] = Map()

    def checkIfSimComputed(u: String, u_to_pred: String): Float ={
      if(sim_new_map.contains(u) && sim_new_map(u).contains(u_to_pred))
        return sim_new_map(u)(u_to_pred)
      else if(sim_new_map.contains(u_to_pred) && sim_new_map(u_to_pred).contains(u))
        return sim_new_map(u_to_pred)(u)
      else
        return 0.0f
    }

    def compute_similarity_map(): Unit = {
      val prints = new PrintWriter(new File(city + "_user_to_user_similarities.txt"))
      var cc: Int = 0
      var s: Float = 0.0f
      for ((k, v) <- user_averages_map)
      {
        for ((k2, v2) <- user_averages_map)
        {
          s = findSim_pearson(user_ratings_map_averaged(k), user_ratings_map_averaged(k2))
          if (s > 0.005f)
            prints.write(k + "," + k2 + "," + s + "\n")
        }
        cc += 1
      }
      prints.close()
    }

    def read_computed_similarity_map(): Unit = {
      val user_similarity_file_path = city + "_user_to_user_similarities.txt"
      sim_new_map = sc.textFile(user_similarity_file_path)
        .map(_.split(","))
        .map(item => (item(0), (item(1), item(2).toFloat)))
        .groupByKey()
        .map(item => (item._1, item._2.toMap))
        .collectAsMap()
    }

    // cases to handle when there is a new user or new business or when there are no two business whose similarity is >0.
    def get_averaged_pred(u_id: String, b_id: String): Float = {
      if (business_clusters_map.contains(b_id) && user_ratings_map.contains(u_id))
        (business_averages_map(b_id) + user_averages_map(u_id) + avg_rating).toFloat / 3.0f
      else if (!business_clusters_map.contains(b_id) && user_ratings_map.contains(u_id))
        (user_averages_map(u_id) + avg_rating).toFloat / 2.0f
      else if (business_clusters_map.contains(b_id) && !user_ratings_map.contains(u_id))
        (business_averages_map(b_id) + avg_rating).toFloat / 2.0f
      else
        avg_rating.toFloat
    }

    def prediction_test(): Unit = {
      val test_data_iterate = test_data.collect()
      var num: Float = 0.0f
      var den: Float = 0.0f
      var pred = 0.0f

      for (item <- test_data_iterate) {
        val user_pred = item._1._1
        val business_pred = item._1._2

        if (business_clusters_map.contains(business_pred) && user_ratings_map.contains(user_pred)) {
          val user_set = business_clusters_map(business_pred)
          var flag = 0
          for (user <- user_set)
          {

            val s = checkIfSimComputed(user, user_pred)
            if (s > 0.00) {
              num += s * (user_ratings_map(user)(business_pred) - user_averages_map(user))
              den += s
              flag = 1
            }
          }
          if (flag == 1) {
            pred = user_averages_map(user_pred) + num / den
          }
          else {
            pred = get_averaged_pred(user_pred, business_pred)
          }
        }
        else {
          pred = get_averaged_pred(user_pred, business_pred)
        }
        test_predictions += ((user_pred, business_pred) -> pred)
      }
    }

    def prediction_train(): Unit = {
      val train_preds = new PrintWriter(new File(city + "_UserBased_train_predictions.txt"))

      val train_data_iterate = train_data
        .map(item => ((item(0), item(1)), item(2).toFloat))
        .collect()
      var num: Float = 0.0f
      var den: Float = 0.0f
      var pred = 0.0f

      for (item <- train_data_iterate) {
        val user_pred = item._1._1
        val business_pred = item._1._2

        if (business_clusters_map.contains(business_pred) && user_ratings_map.contains(user_pred)) {
          val user_set = business_clusters_map(business_pred)
          var flag = 0
          for (user <- user_set)
          {

            val s = checkIfSimComputed(user, user_pred)
            if (s > 0.00) {
              num += s * (user_ratings_map(user)(business_pred) - user_averages_map(user))
              den += s
              flag = 1
            }
          }
          if (flag == 1) {
            pred = user_averages_map(user_pred) + num / den
          }
          else {
            pred = get_averaged_pred(user_pred, business_pred)
          }
        }
        else {
          pred = get_averaged_pred(user_pred, business_pred)
        }

        train_predictions += ((user_pred, business_pred) -> pred)
        train_preds.write(user_pred+ "," + business_pred + "," + pred+"\n")
      }
      train_preds.close()
    }

    def prediction_for_all(): Unit = {
      val test_data_iterate = test_data.collect()
      var num: Float = 0.0f
      var den: Float = 0.0f
      var pred = 0.0f

      val out_predictions = new PrintWriter(new File(city + "_UserBased_all_predictions.txt"))

      for (user <- test_users_set) {

        val user_pred = user
        for (business <- all_businesses_set) {
          val business_pred = business
          if ((!user_ratings_map.contains(user_pred) && !business_clusters_map.contains(business_pred)) || !user_ratings_map.contains(user_pred) || !business_clusters_map.contains(business_pred))
          {
            pred = get_averaged_pred(user_pred, business_pred)
          }
          else if (!user_ratings_map(user_pred).contains(business_pred))
          {
            if (business_clusters_map.contains(business_pred) && user_ratings_map.contains(user_pred))
            {
              val user_set = business_clusters_map(business_pred)
              var flag = 0
              for (users <- user_set)
              {
                if (sim_new_map.contains(user) && sim_new_map(user).contains(users))
                {
                  val s = sim_new_map(user)(users)
                  num += s * (user_ratings_map(users)(business_pred) - user_averages_map(users))
                  den += s
                  flag = 1
                }
              }
              if (flag == 1)
                pred = user_averages_map(user_pred) + num / den
              else
                pred = get_averaged_pred(user_pred, business_pred)
            }
            else
              pred = get_averaged_pred(user_pred, business_pred)
          }
          if (pred >= 0.0)
          {
            out_predictions.write(user_pred + "," + business_pred + "," + pred + "\n")
          }
        }

      }
      out_predictions.close()
    }

    def compute_RMSE_and_print_results(predictions: Map[(String, String), Float], file_type: String): Unit = {
      val new_predictions = sc.parallelize(predictions.toList)
      val to_write_predictions = new_predictions
        .map(item => (item._1._1, item._1._2, item._2))
        .collect()
        .sortWith(customSort)

      var ratesAndPreds = test_data.join(new_predictions)
      if(file_type.equals("Train")) {
          val train_req = train_data.map(item => ((item(0), item(1)) , item(2).toFloat))

//        train_req.take(10).foreach(println)
        ratesAndPreds = train_req.join(new_predictions)
//        ratesAndPreds.take(10).foreach(println)
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

    if(run_mode == 1) {
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
      .map(item => item.split(",")).map(item => ((item(0), item(1)), item(2).toFloat)).persist()

    all_review_rdd = test_rdd
        .union(train_rdd)
  }

  def customSort(s1: (String, String, Float), s2: (String, String, Float)): Boolean = {
    if (s1._1 != s2._1)
      s1._1 < s2._1
    else
      s1._2 < s2._2
  }

  def findSim2_euc(user_1: Map[String, Float], user_2: Map[String, Float]): Float = {

    val common_users = user_1.keySet.intersect(user_2.keySet)

    // if no keys are in common that return similarity as 0
    if (common_users.isEmpty)
      return 0.0f

    // calculate numerators
    var dot_product: Float = 0.0f
    for (item <- common_users)
      dot_product += math.pow(user_1(item) - user_2(item), 2).toFloat

    dot_product = math.sqrt(dot_product).toFloat
    val similarity = math.exp(dot_product) / (math.exp(dot_product) + dot_product)

    similarity.toFloat
  }

  def findSim_pearson(h1: Map[String, Float], h2: Map[String, Float]): Float = {

    var h1_mod = 0.0f
    var h2_mod = 0.0f
    var comb = 0.0f

    val common = h1.keySet.intersect(h2.keySet)
    if (common.isEmpty)
      return 0.0f

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
      return 0.0f
    comb / (h1_mod * h2_mod)
  }

  def findSim_man(user_1: Map[String, Float], user_2: Map[String, Float]): Float = {

    val common_users = user_1.keySet.intersect(user_2.keySet)

    // if no keys are in common that return similarity as 0
    if (common_users.isEmpty)
      return 0

    // calculate numerators
    var dot_product: Float = 0
    for (item <- common_users)
      dot_product += math.abs(user_1(item) - user_2(item))

    val similarity = math.exp(dot_product) / (math.exp(dot_product) + dot_product)

    similarity.toFloat
  }

  def findSim_jaccard(user_1: Map[String, Float], user_2: Map[String, Float]): Float = {

    val common_users = user_1.keySet.intersect(user_2.keySet)

    // if no keys are in common that return similarity as 0
    if (common_users.isEmpty)
      return 0

    // calculate numerators
    var dot_product: Float = 0.0f
    var num: Float = 0.0f
    var denom: Float = 0.0f
    for (item <- common_users) {
      num += math.min(user_1(item), user_2(item))
      denom += math.max(user_1(item), user_2(item))
    }

    val similarity = num / denom
    similarity.toFloat
  }

  def findSim_tanimoto(user_1: Map[String, Float], user_2: Map[String, Float]): Float = {

    val common_users = user_1.keySet.intersect(user_2.keySet)

    // if no keys are in common that return similarity as 0
    if (common_users.isEmpty)
      return 0

    // calculate numerators
    var dot_product: Float = 0.0f
    var num: Float = 0.0f
    var denom: Float = 0.0f
    for (item <- common_users) {
      dot_product += math.abs(user_1(item) - user_2(item))
    }

    var denominator_item_1: Float = 0.0f
    var denominator_item_2: Float = 0.0f

    for ((k, v) <- user_1)
      denominator_item_1 += v * v
    for ((k, v) <- user_2)
      denominator_item_2 += v * v

    val denominator = denominator_item_1 + denominator_item_2 - dot_product
    if (denominator != 0)
      dot_product / denominator
    else
      0.0f
  }

  def print_ranges(ratesAndPreds: RDD[((String, String), (Float, Float))]): Unit = {
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

}