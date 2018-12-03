import java.io.{File, PrintWriter}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

object data_analysis {
  var spark : SparkSession = null
  var sc: SparkContext = null
  def main(args: Array[String]): Unit = {
    //initialise spark configuration and start spark session
    val conf = new SparkConf().setAppName("ModelBasedCF").setMaster("local[*]")
    sc = new SparkContext(conf)

    spark = SparkSession
      .builder()
      .appName("Task1")
      .master("local[*]")
      .getOrCreate()

    // Setting Log level to Error
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

//    analyse_review_data()
//    analyse_review_las_vegas_data()
//    analyse_business_data()

    // Step 1. get business_ids of restaurants and store it into a file
//    get_restaurants_based_on_business_data("Las Vegas", "bus_Las_Vegas_restaurants.txt")

    // Step 2. get reviews for the given business
//    get_reviews_for_given_businesses("bus_Las_Vegas_restaurants.txt",0, 20, 0)

    // divide data into train and test
//    divide_date("las_vegas/las_vegas_review_with_text_50_lemma.txt", "las_vegas/las_vegas_review_with_text_50_lemma_60_40_train.txt", "las_vegas/las_vegas_review_with_text_50_lemma_60_40_test.txt")

    // Step 3. get reviews for the given business with text and remove punctuations and stop words
//    get_reviews_for_given_businesses_with_text("las_vegas_review_with_text_20_res_data.csv", "las_vegas_review_0_20_0_res_data.txt")

    // Step 4. Run the python script to lemmatize the file generated in step 3

    // Step 5. get words from the text file with count k or less
//    get_rare_words_with_count_k("las_vegas/las_vegas_review_with_rare_removed_text_50_lemma.txt", "las_vegas/temp_rare.txt", 1)

    // step 6. remove words with single occurrences
//    remove_rare_words_from_data("las_vegas/las_vegas_review_with_rare_removed_text_50_lemma.txt", "las_vegas/las_vegas_rare_words.txt", "las_vegas/las_vegas_review_with_text_50_lemma.txt")

//    creating_large_file()
//    demo_class()

//    write_business_attributes()
//    test_func2()
//    get_business_ids()
//    split_data_randomly("las_vegas/las_vegas_review_with_rare_removed_text_50_lemma.txt","las_vegas/las_vegas_review_with_rare_removed_text_50_lemma_80_20_train.txt","las_vegas/las_vegas_review_with_rare_removed_text_50_lemma_80_20_test.txt")

//    remove_quotes()
//    creating_large_file()
  }

  def get_reviews_for_given_businesses(city_bus_file_name: String, min_no_of_reviews_per_business : Int,min_no_of_reviews_per_user: Int, avg_rating_req: Int): Unit = {
    val x = ""+min_no_of_reviews_per_business+"_"+min_no_of_reviews_per_user+"_"+avg_rating_req
    val write_path = "/Users/dharmik/Desktop/Semester3/Data Mining/final_project/src/main/Dharmik_Data/las_vegas_review_"+x+"_res_data.txt"
    val printWriter = new PrintWriter(new File(write_path))
//    val review_data_path = "/Users/dharmik/Desktop/Semester3/Data Mining/Extra Material/yelp_dataset/yelp_academic_dataset_review.json"
    val final_review_path = "/Users/dharmik/Desktop/Semester3/Data Mining/final_project/src/main/Dharmik_Data/final_review_data.txt"
//    val review_df = spark.read.text(review_data_path)
    val review_rdd = sc.textFile(final_review_path)
    val refinement = 1

    val business_ids_path = "/Users/dharmik/Desktop/Semester3/Data Mining/final_project/src/main/Dharmik_Data/"+ city_bus_file_name
    val business_ids_set = spark.read.text(business_ids_path)
      .rdd
      .map(item => item.toString())
      .collect()
      .toSet

    val data = review_rdd
      .map(item => item.split(","))
      .map(item => (item(0), item(1), item(2)))
      .filter(item => business_ids_set.contains("[" + item._2 + "]"))
      .persist()

    val unique_users = data
      .map(item => (item._1, 1))
      .reduceByKey(_+_)
      .count()

    val unique_businesses = data
      .map(item => (item._2,1))
      .reduceByKey(_+_)
      .count()

    println("Total Unique_users = "+unique_users)
    println("Total Unique_Busniess = "+unique_businesses)

    if (refinement == 1) {
      val users = data.map(item => (item._1, 1))
        .reduceByKey(_ + _)
        .filter(item => item._2 > min_no_of_reviews_per_user)
        .map(item => item._1)
        .collect()
        .toSet

      val users_with_avg_more_than_n = data
        .map(item => (item._1, item._3.toInt))
        .groupByKey()
        .map(item => (item._1, item._2.toList.sum * 1.0/ item._2.size))
        .filter(item => item._2 > avg_rating_req)
        .map(item => item._1)
        .collect()
        .toSet

      val busniess_with_at_least_k_reviews = data
        .map(item => (item._2, 1))
        .reduceByKey(_+_)
        .filter(_._2 > min_no_of_reviews_per_business)
        .map(item => item._1)
        .collect()
        .toSet

      println("users_with_avg_more_than_"+ avg_rating_req+" = "+users_with_avg_more_than_n.size)
      println("busniess_with_at_least_k_reviews ="+busniess_with_at_least_k_reviews.size)

      val reviews = data
        .filter(item => users.contains(item._1) && users_with_avg_more_than_n.contains(item._1) && busniess_with_at_least_k_reviews(item._2))
        .persist()

//      val print_users = reviews
//        .map(item => (item._1, 1))
//        .reduceByKey(_+_)
//        .collect()
//        .foreach(println)

//      println()
//      println()

//      val print_businesses = reviews
//        .map(item => (item._2, 1))
//        .reduceByKey(_+_)
//        .collect()
//        .foreach(println)

      for (item <- reviews.collect()) {
        //        println(item._1 +","+item._2+","+item._3+"\n")
        printWriter.write(item._1 + "," + item._2 + "," + item._3 + "\n")
      }
      val ref_unique_users = reviews
        .map(item => (item._1, 1))
        .reduceByKey(_+_)
        .count()

      val ref_unique_businesses = reviews
        .map(item => (item._2,1))
        .reduceByKey(_+_)
        .count()

      println("Refined Unique_users = "+ref_unique_users)
      println("Refined Unique_Busniess = "+ref_unique_businesses)
    }
    else {
      for (item <- data.collect()) {
        //        println(item._1 +","+item._2+","+item._3+"\n")
        printWriter.write(item._1 + "," + item._2 + "," + item._3 + "\n")
      }
    }
    printWriter.close()
  }

  def analyse_business_data_for_restaurants(business_ids_file_name: String ): Unit ={
    val business_data_path = "/Users/dharmik/Desktop/Semester3/Data Mining/Extra Material/yelp_dataset/yelp_academic_dataset_business.json"
    val busniess_df = spark.read.json(business_data_path)
    val business_rdd = busniess_df.rdd

    val business_ids_path = "/Users/dharmik/Desktop/Semester3/Data Mining/final_project/src/main/Dharmik_Data/" + business_ids_file_name
    val busniess_ids_set = sc.textFile(business_ids_path)
      .collect()
      .toSet

    def custom(s1: String): Boolean ={
      return s1.equalsIgnoreCase("Pittsburgh")
    }

    // Business ids where city = "Las Vegas"
    val bus = business_rdd
      .map(item => (item(4).toString, (item(2).toString, 1)))
      .filter(item => custom(item._1))
      .map(item => item._2._1)
      .collect()
      .foreach(println)
  }

  def get_restaurants_based_on_business_data(City: String, write_to_file: String) {

    val write_path = "/Users/dharmik/Desktop/Semester3/Data Mining/final_project/src/main/Dharmik_Data/" + write_to_file
    val printWriter = new PrintWriter(new File(write_path))

    val business_data_path = "/Users/dharmik/Desktop/Semester3/Data Mining/Extra Material/yelp_dataset/yelp_academic_dataset_business.json"
    val busniess_df = spark.read.json(business_data_path)
    val business_rdd = busniess_df.rdd

    val categories_keywords = "lithuanian, azerbaijani, slovenian, latin american, thai, yamal, rajasthani, himalayan/nepalese, buddhist, " +
      "southern, karaoke, afghan, wok, belgian, steakhouses, gluten-free, singaporean, keralite, slovakian, bakeries, coffee & tea, serbian, " +
      "greek, french, malaysian, new mexican cuisine, korean, laotian, georgian, fast-food, chicken wings, pashtun, cajun, chechen, zanzibari, " +
      "indonesian, peranakan, kosher, buffets, belarusian, mangalorean, seafood, modern european, wine bars, lebanese, pennsylvania, sri lankan, " +
      "tapas barssports bars, halal, barbeque, native, assyrian, vegetarian, estonian, asian fusion, anglo-indian, udupi, cafes, ethiopian, " +
      "burmese, moroccan, islamic, germanfrench, filipino, mughal, poutineries, shanghainese, awadhi, food court, jewish, supper clubs, turkish, " +
      "breakfast & brunch, asian fusionpizza, nightlife, cuisine, szechuan, slovak, andhra, polish, russian, middle eastern, tamil, bakeries, " +
      "romanian, tapas/small plates, sami, gastropubs, tatar, parsi, ukrainian, mediterranean, brazilian, cantonese, argentina, restaurants, somali, " +
      "caterers, albanian, south indian, bars, food trucks, dutch, danish, juice bars & smoothies, armenian, food, odia, falafel, mordovian, " +
      "circassian, malay, czech, ramen, ainu, cocktail bars, crimean, burgers, dim sum, vegan, indian, hungarian, pakistani, maharashtrian, cambodian, " +
      "sandwiches, austrian, kebab, american, caribbean, pop-up restaurants, louisiana, punjabi, catholic, delis, pizza, berber, hyderabad, " +
      "italian, kurdish, sushi bars, arabian, creole, new mexican, hot-dogs, bulgarian, breweries, bangladeshi, pasta, food stands, latvian, tex-mex, " +
      "american (traditional), inuit, ice cream & frozen yogurt, sindhi, goan, peruvian, persian, japanese, mexican, nepalese, arab, portuguese, " +
      "japanese curry, american (new), bengali, australian, organic stores, chinese, karnataka, kazakh,coffee roasteries, " +
      "coffee roasteries, coffee & tea, chocolatiers & shops, specialty food, coffee & tea supplies, caterers, food," +
      "nightlife, restaurants, bars, cocktail bars, american (traditional),food delivery services, restaurants, caterers, food, chinese,salad,coffee & tea, " +
      "donuts, breakfast & brunch, restaurants, food" +
      "breakfast, lunch, dinner, cajun/creole,american (traditional), comfort food, barbeque, restaurants,arts & entertainment, music venues, nightlife, bars, dance clubs," +
      "bars, nightlife, lounges, dance clubs,coffee,tea"


    val catogeries_keywords_set = categories_keywords.split(",")
      .map(item => item.trim().toLowerCase)
      .toSet

    def custom(s1: String): Boolean ={
      return s1.equalsIgnoreCase(City)
    }

    def custom_res(strings: Set[String]): Boolean={
      if(strings.intersect(catogeries_keywords_set).size == 0)
        false
      else
        true
    }

    println("total_busniesses = ", business_rdd.count())

//    val categories = business_rdd
//        .filter(item => item(3) != null && custom_res(item(3).toString.split(",").map(item => item.trim.toLowerCase).toSet))
//        .count()
//
//    println("total restaurants = "+ categories)

//     Business ids where city = "Pittsburgh"
    val bus = business_rdd
      .filter(item => item(3) != null && custom_res(item(3).toString.split(",").map(item => item.trim.toLowerCase).toSet) && custom(item(4).toString))
      .map(item => item(2).toString)
      .collect()
      .foreach(item => printWriter.write(item + "\n"))

    printWriter.close()
//    println("bus length = "+bus)

}

  def analyse_user_data(): Unit ={
    val user_data_path = "/Users/dharmik/Desktop/Semester3/Data Mining/Extra Material/yelp_dataset/yelp_academic_dataset_business.json"
    val user_df = spark.read.json(user_data_path)
    val user_rdd = user_df.rdd

    println(user_rdd.count())

  }

  def split_data_randomly(entire_data_path: String, train_path: String, test_path: String): Unit ={
    val data_path = "/Users/dharmik/Desktop/Semester3/Data Mining/final_project/src/main/Dharmik_Data/" + entire_data_path
    val train_data_path = "/Users/dharmik/Desktop/Semester3/Data Mining/final_project/src/main/Dharmik_Data/" + train_path
    val test_data_path = "/Users/dharmik/Desktop/Semester3/Data Mining/final_project/src/main/Dharmik_Data/" + test_path

    val train_writer = new PrintWriter(new File(train_data_path))
    val test_writer  = new PrintWriter(new File(test_data_path))

    val data_full = sc.textFile(data_path)
      .randomSplit(Array(0.8, 0.2), 11L)

    val train = data_full(0)
    val test = data_full(1)

    train
      .collect()
      .foreach(item => train_writer.write(item +"\n"))

    test
      .collect()
      .foreach(item => test_writer.write(item +"\n"))

    train_writer.close()
    test_writer.close()

  }

  def get_reviews_for_given_businesses_with_text(write_to_file:String, reviews_with_only_rating_file: String)={
    val write_path = "/Users/dharmik/Desktop/Semester3/Data Mining/final_project/src/main/Dharmik_Data/" +write_to_file
    val printWriter = new PrintWriter(new File(write_path))
    val review_data_path = "/Users/dharmik/Desktop/Semester3/Data Mining/Extra Material/yelp_dataset/yelp_academic_dataset_review.json"
    val review_df = spark.read.json(review_data_path)

    val custom_partitionor = new HashPartitioner(12000)

    val review_rdd = review_df.rdd

    val stop_words = sc.textFile("/Users/dharmik/Desktop/Semester3/Data Mining/final_project/src/main/Dharmik_Data/stop_words.txt")
      .collect()
      .toSet

    def remove_punctuation(word: String): String ={
      val punctuations: Set[Char] = Set('!', '@', '#', '$', '%', '^', '&','*', '(', ')', '-', '_',
        '+', '=', '`', '~', '{', '}', ':', ';','\"', '[', ']', '\\', '|', '<', '>',
        ',', '.', '/', '?')

      val str = new StringBuilder()
      for(character <- word){
        if(!punctuations.contains(character))
          str.append(character)
        else
          str.append(' ')
      }
      str.toString()
    }

    def filter_review(line:String): String ={
      val arr: Array[String] = line.split(" ")

      val new_str = new StringBuilder()
      for(item <- arr) {
        val clean_item = remove_punctuation(item)
        new_str.append(clean_item + " ")
      }
      new_str.toString().replaceAll(" +", " ")
    }

    val reviews_with_only_rating_file_path = "/Users/dharmik/Desktop/Semester3/Data Mining/final_project/src/main/Dharmik_Data/" + reviews_with_only_rating_file
    val reviews_with_only_rating_data = spark.read.text(reviews_with_only_rating_file_path)

    val reviews_with_only_rating_rdd = reviews_with_only_rating_data.rdd
      .map(item => item.toString().substring(1, item.toString().length - 1).split(","))
      .map(item=> ((item(0), item(1)) , item(2)))
//      .take(1)
//      .foreach(println)

    val partitioned_review_rdd = review_rdd
      .map(item => (item(8).toString, (item(0).toString, item(5).toString, item(6).toString, item(2).toString, item(7).toString, item(3).toString, item(1).toString)))
      .partitionBy(custom_partitionor)

    val reviews_with_text_rdd = partitioned_review_rdd
      .map(item => ((item._1, item._2._1),(item._2._2, filter_review(item._2._3.toLowerCase.replaceAll("\"", " ")
      .filter(_ >= ' '))
      ,item._2._4, item._2._5, item._2._6, item._2._7)))

//    val temp = reviews_with_text_rdd

//      .take(1)
//      .foreach(println)

    val combined_rdd = reviews_with_only_rating_rdd
      .join(reviews_with_text_rdd)
      .map(item => ("\""+item._1._1+"\"", "\""+item._1._2+"\"", "\""+item._2._1+"\"", "\""+item._2._2._2+"\"", "\""+item._2._2._3+"\"", "\""+item._2._2._4+"\"", "\""+item._2._2._5+"\"", "\""+item._2._2._6+"\""))
      .collect
      .foreach(item => printWriter.write(item._1+","+item._2+","+item._3+","+item._5+","+item._6+","+item._7+","+item._8+","+item._4+"\n"))
//      combined_rdd
//      .foreach(item => printWriter.write(""+item._1+","+item._2+","+item._3+","+item._4+"\n"))

//    var cc = 0
//    for(item <- combined_rdd)
//      {
//
//        val str = item._4
//          .replaceAll("\n", " ")
//          .replaceAll("\\n\\r", " ")
//          .replaceAll("\n\r" , " ")
//
//        printWriter.write(item._1+","+item._2+","+item._3+","+item._5+","+item._6+","+item._7+","+item._8+","+item._4+"\n")
//
//        if(str.contains("\n"))
//          println(cc)
//
//        cc+=1
//      }

//    var count = 1
//    for(item <- combined_rdd){
//      if(item.toString() == "\n")
//      {
//        println(count)
//        count+=1
//      }
//    }


    printWriter.close()

  }

  def remove_rare_words_from_data(write_to_file: String, rare_words_file: String, file_name:String): Unit = {
    val write_path = "/Users/dharmik/Desktop/Semester3/Data Mining/final_project/src/main/Dharmik_Data/" + write_to_file
    val printWriter = new PrintWriter(new File(write_path))

    val review_data_path = "/Users/dharmik/Desktop/Semester3/Data Mining/final_project/src/main/Dharmik_Data/" + file_name
    val review_df = spark.read.csv(review_data_path)

    val custom_partitionor = new HashPartitioner(12000)

    val review_rdd = review_df.rdd

    val rare_words = sc.textFile("/Users/dharmik/Desktop/Semester3/Data Mining/final_project/src/main/Dharmik_Data/"+ rare_words_file)
      .collect()
      .toSet

//    print(rare_words)

    def filter_review(line:String): String ={
      val arr: Array[String] = line.split("\\s+")
      val new_str = new StringBuilder()
//      var flag = 0
      for(item <- arr)
      {

        if(!rare_words.contains(item))
        {
          new_str.append(item + " ")
        }
//        else{
//          flag = 1
//        }
      }
//      if(flag == 1){
//        print(line)
//        print(new_str.toString())
//      }
      new_str.toString().replaceAll(" +", " ")
    }

    val partitioned_review_rdd = review_rdd
      .map(item => (item(0).toString, (item(1).toString, item(2).toString, item(3).toString, item(4).toString, item(5).toString, item(6).toString, if(item(7) != null) item(7).toString else "")))
      .partitionBy(custom_partitionor)

    val reviews_with_text_rdd = partitioned_review_rdd
      .map(item => ((item._1, item._2._1),(item._2._2, item._2._3, item._2._4, item._2._5, item._2._6, filter_review(item._2._7.toLowerCase.replaceAll("\"", " ")
        .filter(_ >= ' ')))))

    val combined_rdd = reviews_with_text_rdd
      .map(item => ("\""+item._1._1+"\"", "\""+item._1._2+"\"", "\""+item._2._1+"\"", "\""+item._2._2+"\"", "\""+item._2._3+"\"", "\""+item._2._4+"\"", "\""+item._2._5+"\"", "\""+item._2._6+"\""))
      .collect()
      .sortBy(item => item._1)

    //      combined_rdd
    //      .foreach(item => printWriter.write(""+item._1+","+item._2+","+item._3+","+item._4+"\n"))

    for(item <- combined_rdd)
    {
      printWriter.write(item._1+","+item._2+","+item._3+","+item._4+","+item._5+","+item._6+","+item._7+","+item._8+"\n")
    }

    //    var count = 1
    //    for(item <- combined_rdd){
    //      if(item.toString() == "\n")
    //      {
    //        println(count)
    //        count+=1
    //      }
    //    }

    println(combined_rdd.length)

    printWriter.close()

  }

  def verify_file(): Unit = {
    val file_name = "/Users/dharmik/Desktop/Semester3/Data Mining/final_project/src/main/Dharmik_Data/pittsburgh_review_with_text_20_res_data.csv"
    val file_rdd = sc.textFile(file_name)
      .map(item => item.split(","))
      .map(item=> (item(0), item(1), item(2), item(3)))
      .collect()
  }

  def creating_large_file(): Unit ={
    val user_file_name = "/Users/dharmik/Desktop/Semester3/Data Mining/Extra Material/yelp_dataset/yelp_academic_dataset_user.json"
    val business_file_name = "/Users/dharmik/Desktop/Semester3/Data Mining/Extra Material/yelp_dataset/yelp_academic_dataset_business.json"
    val review_file_name = "/Users/dharmik/Desktop/Semester3/Data Mining/final_project/src/main/Dharmik_Data/las_vegas/las_vegas_review_with_text_50_lemma.csv"
    val print_results = "/Users/dharmik/Desktop/Semester3/Data Mining/final_project/src/main/Dharmik_Data/extras/combined_results_las_vegas.txt"

    val pw = new PrintWriter(new File(print_results))

    val user_df = spark.read.json(user_file_name)
    val business_df = spark.read.json(business_file_name).select("business_id", "stars", "review_count",
      "is_open", "latitude", "longitude", "categories")
    val review_df = spark.read.csv(review_file_name)

    val user_rdd = user_df.rdd
      .map(row => (row(20).toString,(row(17).toString, row(0).toString, row(13).toString, row(18).toString, row(21).toString)))
//      .take(10)
//      .foreach(println)

    println(); println()

    val business_rdd = business_df.rdd
      .map(item => (item(0).toString, (item(1), item(2), item(3), item(4), item(5), item(6))))
//      .take(10)
//      .foreach(println)
//      .map(item => (item(2).toString, (item(13).toString, item(12).toString, item(6).toString, item(3).toString, item(7).toString, item(8).toString, item(1).toString)))

    println(); println()

    val review_rdd = review_df.rdd
      .map(row => (row(0).toString, (row(1).toString, row(2).toString, row(3).toString , row(4).toString , row(5).toString , row(6).toString , row(7).toString)))
//      .take(10)
//      .foreach(println)

    val combined_u_r = user_rdd
      .join(review_rdd)
      .map(item => (item._2._2._1.toString, (item._1, item._2._1, item._2._2)))
      .join(business_rdd)
      .map(item => (item._2._1._1, (item._2._1._2, (item._1, item._2._2), item._2._1._3)))
      .collect()
      .sortBy(_._1)
      .foreach(item => pw.write(item._1+","+item._2 +"\n"))

    pw.close()

  }

  def demo_class(): Unit = {
    val words_rdd = sc.textFile("/Users/dharmik/Desktop/Semester3/Data Mining/final_project/src/main/Dharmik_Data/stop_words.txt")

    val stop_words = words_rdd
      .collect
      .toSet

    def remove_punctuation(word: String): String ={
      val punctuations: Set[Char] = Set('!', '@', '#', '$', '%', '^', '&','*', '(', ')', '-', '_',
        '+', '=', '`', '~', '{', '}', ':', ';','\"', '[', ']', '\\', '|', '<', '>',
        ',', '.', '/', '?')

      val str = new StringBuilder()
      for(character <- word){
        if(!punctuations.contains(character))
          str.append(character)
        else
          str.append(' ')
      }

      val arr = str.toString().trim.split(" ")
      val stri = new StringBuilder()
      for(item <- arr)
        if(!stop_words.contains(item))
          stri.append(item +" ")
      stri.toString().trim
    }

    def filter_review(line:String): String ={
      val arr: Array[String] = line.split(" ")

      val new_str = new StringBuilder()
      for(item <- arr) {
        val clean_item = remove_punctuation(item)
        new_str.append(clean_item + " ")
      }
      new_str.toString().replaceAll(" +", " ")
    }

    val old_string = "this is not my first rodeo at lin's, but it is the first time my husband and i have been here with their sushi bar open. i started with egg flower soup, which is pretty standard as an egg drop soup goes but served very hot (keep the ice water close by!). he put in an order for a blue crab philadelphia roll at the same time we ordered our entrees, white meat sesame chicken / broccoli and vegetable lo mein. the chicken is delicious and their lo mein is always spot on. the roll didn't come until after the entrees and he wasn't impressed. the roll was much too large and the presentation was sloppy. no soy sauce for dipping was brought to the table until requested and there was less than a pea sized dollop of wasabi. he said he would have sushi here again but they have a lot of work to do. hopefully this service will improve with more experience. overall, lin's is a good choice for chinese food but nakama or little tokyo is preferential for sushi as of now."

    val new_string = filter_review(old_string)
    println(new_string)
  }

  def get_rare_words_with_count_k(data_file: String, rare_words_file: String, k: Int): Unit ={
    val file_name = "/Users/dharmik/Desktop/Semester3/Data Mining/final_project/src/main/Dharmik_Data/" + data_file
//    val file_name = "/Users/dharmik/Desktop/Semester3/Data Mining/final_project/src/main/Dharmik_Data/pittsburgh_review_with_text_20_res_data_copy.csv"

    val write_path = "/Users/dharmik/Desktop/Semester3/Data Mining/final_project/src/main/Dharmik_Data/" + rare_words_file
    val printWriter = new PrintWriter(new File(write_path))


    val file_rdd = spark.read.csv(file_name)
      .rdd
      .map(row => if(row(7) != null)row(7) else "")
//        .take(10)
//        .foreach(println)
      .flatMap(line => line.toString.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_+_)
      .sortBy(_._2)
      .filter(_._2< (k+1))
      .map(item => item._1)
      .collect()
      .foreach(item => printWriter.write(item + "\n"))

//    println("total length = "+file_rdd)

    printWriter.close()

  }

  def write_business_attributes(): Unit ={
    val business_file_name = "/Users/dharmik/Desktop/Semester3/Data Mining/Extra Material/yelp_dataset/yelp_academic_dataset_business.txt"
    val business_file_rdd = sc.textFile(business_file_name)
      .take(2)
      .foreach(println)

  }

  def test_func(): Unit = {
    val data_path = "src/main/Dharmik_Data/las_vegas/las_vegas_20_text_random_75_25/las_vegas_review_with_text_20_lemma.csv"
    val write_path = "src/main/Dharmik_Data/las_vegas/las_vegas_review_with_text_50_lemma.txt"

    val Pwriter = new PrintWriter(new File(write_path))

    val data_rdd = spark.read.csv(data_path)
      .rdd
      .cache()

    val user_set = data_rdd
      .map(item => (item(0).toString, 1))
      .reduceByKey(_+_)
      .filter(_._2 > 50)
      .map(item => item._1)
      .collect()
      .toSet

    val new_data = data_rdd
      .filter(item => !user_set(item(0).toString))
      .cache()

    new_data
      .collect()
      .foreach(item => Pwriter.write(item.toString().substring(1, item.toString().length -2)+"\n"))
//      .foreach(item => Pwriter.write(item(0)+","+item(1)+","+item(2)+","+item(3)+","+item(4)+","+item(5)+","+item(6)+","+item(1)+"\n"))

    val total_users = new_data
        .map(item => (item(0).toString, 1))
        .reduceByKey(_+_)
        .collect()
        .length

    val total_bus = new_data
      .map(item => (item(1).toString, 1))
      .reduceByKey(_+_)
      .collect()
      .length

    println("total_users = " + total_users)
    println("total_bus = " + total_bus)

    Pwriter.close()
  }

  def test_func2(): Unit ={

    val data_path = "/Users/dharmik/Desktop/Semester3/Data Mining/final_project/src/main/Dharmik_Data/las_vegas/las_vegas_review_with_rare_removed_text_50_lemma.txt"
    val train_data_path = "/Users/dharmik/Desktop/Semester3/Data Mining/final_project/src/main/Dharmik_Data/las_vegas/las_vegas_review_with_rare_removed_text_50_lemma_80_20_train.txt"
    val test_data_path = "/Users/dharmik/Desktop/Semester3/Data Mining/final_project/src/main/Dharmik_Data/las_vegas/las_vegas_review_with_rare_removed_text_50_lemma_80_20_test.txt"

    val data_rdd = spark.read.csv(data_path)
      .rdd
      .cache()

    val train_data_rdd = spark.read.csv(train_data_path)
      .rdd
      .cache()

    val test_data_rdd = spark.read.csv(test_data_path)
      .rdd
      .cache()

    val splits = data_rdd
      .randomSplit(Array(0.75, 0.25), 11L)

    val train = splits(0)
    val test = splits(1)

    val total_users = data_rdd
      .map(item => (item(0).toString, 1))
      .reduceByKey(_+_)
      .collect()
      .length

    val total_bus = data_rdd
      .map(item => (item(1).toString, 1))
      .reduceByKey(_+_)
      .collect()
      .length

    println("total_users = " + total_users)
    println("total_bus = " + total_bus)

    val total_users_train = train_data_rdd
      .map(item => (item(0).toString, 1))
      .reduceByKey(_+_)
      .collect()
      .length

    val total_bus_train = train_data_rdd
      .map(item => (item(1).toString, 1))
      .reduceByKey(_+_)
      .collect()
      .length

    println("total_users_train = " + total_users_train)
    println("total_bus_train = " + total_bus_train)

    val total_users_test = test_data_rdd
      .map(item => (item(0).toString, 1))
      .reduceByKey(_+_)
      .collect()
      .length

    val total_bus_test = test_data_rdd
      .map(item => (item(1).toString, 1))
      .reduceByKey(_+_)
      .collect()
      .length

    println("total_users_test = " + total_users_test)
    println("total_bus_test = " + total_bus_test)


  }

  def get_business_ids(): Unit ={
    val data_path = "/Users/dharmik/Desktop/Semester3/Data Mining/final_project/src/main/Dharmik_Data/las_vegas/las_vegas_20_text_random_75_25/las_vegas_review_with_text_without_rare_20_res_lemma_data_new.txt"

    val data_rdd = sc.textFile(data_path)
      .map(item => (item.split(",")(1), 1))
      .reduceByKey(_+_)
      .map(item => item._1)
      .collect()
      .foreach(println)
  }

  def temp(): Unit ={
    val file_name = "/Users/dharmik/Desktop/Semester3/Data Mining/Extra Material/yelp_dataset/yelp_academic_dataset_user.json"
    val user_rdd = spark.read.json(file_name)
      .rdd
      .map(item => true)
      .collect()
      .length

    println(user_rdd)
  }

}
