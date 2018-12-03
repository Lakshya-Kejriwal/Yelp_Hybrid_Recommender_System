import pyspark
from pyspark.sql import SparkSession
from operator import add
import matplotlib
import sys
import nltk
nltk.download('punkt')
nltk.download('wordnet')
import json
from nltk.tokenize import sent_tokenize, word_tokenize
from nltk.stem import WordNetLemmatizer
wordnet_lemmatizer = WordNetLemmatizer()


sc = pyspark.SparkContext('local[*]')

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .getOrCreate()


def main():
    # analyse_business_data()
    # analyse_user_data()
    # analyse_review_data()
    # analyse_user_data()
    # clean_big_file()
    # write_business_attributes()
    remove_quotes()

def analyse_business_data():
    business_line_json_path = \
        "/Users/dharmik/Desktop/Semester3/Data Mining/Extra Material/yelp_dataset/yelp_academic_dataset_business.json"
    df = spark.read.json(business_line_json_path)

    rdd = df.rdd.map(tuple)
    # print("Total Business = ", rdd.count())

    def custom_mapper(zipcode):
        return zipcode[0:2]

    def restaurant_matcher(rest_str):
        all_words = list()
        words = rest_str.split(",")
        for word in words:
            all_words.append(word.split(" "))

        word_set = set(all_words)

        top_words = set("Tours")

        common_words = word_set.intersection(top_words)

        if len(common_words) > 0:
            return True
        return False

    # restaurants = rdd.map(lambda r: (r[2], r[3])).filter(lambda r: restaurant_matcher(r[1]))

    print("Business Attributes = ",)
    print("Total Cities = ", rdd.map(lambda r: (custom_mapper(r[11]), 1)).reduceByKey(add).sortBy(lambda r: r[1], ascending=False).collect())
    # print("Total States = ", rdd.map(lambda r: (r[14], 1)).reduceByKey(add).count())
    # print("Total zipcodes = ", rdd.map(lambda r: (r[11], 1)).reduceByKey(add).count())

    # print("Las Vegas count", rdd.filter(lambda r: r[4] == "Las Vegas").map(lambda r: r[4]).count())
    # print("Montreal count", rdd.filter(lambda r: r[4] == "Montreal").map(lambda r: r[4]).count())


def clean_big_file():
    review_file = open("/Users/dharmik/Desktop/Semester3/Data Mining/final_project/src/main/Dharmik_Data/las_vegas_review_with_text_20_res_data.csv")
    write_file = open("/Users/dharmik/Desktop/Semester3/Data Mining/final_project2/Dharmik/las_vegas_review_with_text_20_lemma.txt", 'w+')


    count = 0
    for line in review_file.readlines():
        new_str = ""
        arr = line.strip().split(",")
        review = arr[7].replace("\'", "")
        count += 1
        print(count)
        words = word_tokenize(review)
        words = words[1:-1]
        for word in words:
            # print(type(words))
            new_str += wordnet_lemmatizer.lemmatize(word, pos="v") + " "
            # print(word, wordnet_lemmatizer.lemmatize(word, pos="v"))
        # exit(0)
        final_str = arr[0]+","+arr[1] +","+ arr[2] +","+ arr[3] +","+ arr[4] +","+ arr[5] +","+ arr[6] + ",\"" + new_str +"\""
        write_file.write(final_str +"\n")

    write_file.close()


def remove_quotes():
    total_file = open("/Users/dharmik/Desktop/Semester3/Data Mining/final_project/src/main/Dharmik_Data/las_vegas/las_vegas_review_with_rare_removed_text_50_lemma_80_20_train.txt")
    write_file = open("/Users/dharmik/Desktop/Semester3/Data Mining/final_project/src/main/Dharmik_Data/las_vegas/las_vegas_review_with_rare_removed_text_50_lemma_80_20_train_new.txt", "w+")

    with total_file as file:
        count = 0
        for line in file:
            print(count)
            count += 1
            new_str = ""
            arr = line.split(",")
            for i in range(len(arr)):
                item = arr[i]
                if i != len(arr) - 1:
                    new_str += item[1:-1] + ","
                else:
                    new_str += item[1:-2]

            write_file.write(new_str + "\n")

    write_file.close()


def write_business_attributes():
    file_name = "/Users/dharmik/Desktop/Semester3/Data Mining/Extra Material/yelp_dataset/yelp_academic_dataset_business.txt"

    bus_ids_file = open("/Users/dharmik/Desktop/Semester3/Data Mining/final_project/src/main/Dharmik_Data/las_vegas/las_vegas_20_text_random_75_25/bus_ids_20").readlines()
    bus_ids_set = set()
    for item in bus_ids_file:
        bus_ids_set.add(item.rstrip('\n'))

    write_file = open("las_vegas_20_bus_attributes_categories.txt", 'w+')

    with open(file_name) as file:
        count = 0
        for line in file:
            bus_obj = json.loads(line)
            bus_id = bus_obj['business_id']
            bus_categories = bus_obj['categories']
            final_string = ""
            if bus_id in bus_ids_set:
                print(count)
                count += 1
                if bus_obj['attributes'] is None:
                    write_file.write("\"" + bus_id + "\",\"\"" + "\n")
                    continue
                # print(bus_obj['attributes'])
                str_to_append = ""
                for key in bus_obj['attributes']:
                    # print(key, bus_obj['attributes'][key])
                    if bus_obj['attributes'][key] == "True":
                        str_to_append += key + " "
                    elif bus_obj['attributes'][key][0] == "{":
                        arr = bus_obj['attributes'][key][1:-1].replace(" ", "").split(",")
                        for item in arr:
                            new_items = item.split(":")
                            if new_items[1] == "True":
                                str_to_append += key + "_" + new_items[0][1:-1] + " "
                    elif bus_obj['attributes'][key] != "False" and bus_obj['attributes'][key] != "no":
                        str_to_append += key + "_" + bus_obj['attributes'][key] + " "


                bus_categories_str = ""
                if bus_categories is not None:
                    if "American (Traditional)" in bus_categories:
                        bus_categories = bus_categories.replace("American (Traditional)", "american_traditional")
                    if "American (New)" in bus_categories:
                        bus_categories = bus_categories.replace("American (New)", "american_new")
                    bus_categories_arr = bus_categories.split(",")
                    for item in bus_categories_arr:
                        bus_categories_str += item.strip() + " "

                    # print()

                final_string += "\""+bus_categories_str + " " + str_to_append+"\""
                final_string = final_string.replace("&", "")
                final_string = final_string.replace("Alcohol_none", "").lower().strip()
                # final_string = final_string.replace("\\s+", " ")

                lis = final_string.split()
                # print(lis)
                del lis[len(lis)-1:]
                # print(lis)

                write_file.write("\""+bus_id+"\","+' '.join(lis)+"\"\n")
                # print(final_string)
                # exit(0)

    write_file.close()

    # print(res_lis)


main()
