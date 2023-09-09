from util import *
import pyspark
from pyspark.sql import SparkSession

# Load users and transactions data
spark = SparkSession.builder.appName("SparkAssignment1").getOrCreate()

#loading user_csv file
user_df = spark.read.option("header",True).csv("file:/C:/Users/ASUS/PycharmProjects/pythonProject6/SparkRepo1/resource/user.csv")

#loading transaction_csv file
transaction_df = spark.read.option("header",True).csv("file:/C:/Users/ASUS/PycharmProjects/pythonProject6/SparkRepo1/resource/transaction.csv")

total_df=merge(user_df,transaction_df)

# a): Count of unique locations where each product is sold
UniqueLocation(total_df)
# b): Find out products bought by each user
ProductBought(total_df)
# c): Total spending done by each user on each product
TotalSpending(total_df)