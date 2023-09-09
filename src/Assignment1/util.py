from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct, collect_list, sum
from pyspark.sql.window import Window
import pyspark.sql.functions as F

# Create a SparkSession
def Assignment1():
    spark = SparkSession.builder.appName("SparkAssignment1").getOrCreate()

    # Load users and transactions data
    user_df = spark.read.option("header",True).csv("file:/C:/Users/ASUS/PycharmProjects/pythonProject6/resource/user.csv")
    transaction_df = spark.read.option("header",True).csv("file:/C:/Users/ASUS/PycharmProjects/pythonProject6/resource/transcation.csv")

    total_df = user_df.join(transaction_df, user_df.user_id == transaction_df.userid, )
    # a): Count of unique locations where each product is sold
    product_locations_df = total_df.groupBy("product_id").agg(countDistinct("location ").alias("UniqueLocations"))
    product_locations_df.show()

    # b): Find out products bought by each user
    user_products_df = total_df.groupBy("userid").agg(collect_list("product_id").alias("ProductsBought"))
    user_products_df.show()

    # c): Total spending done by each user on each product
    user_product_spending_df = total_df.groupBy("userid", "product_id").agg(sum("price").alias("TotalSpending"))
    user_product_spending_df.show()

    # Stop the SparkSession
    spark.stop()
