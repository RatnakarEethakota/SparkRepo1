import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct, collect_list, sum

# Create a SparkSession
def merge(user_df,transaction_df):
    total_df = user_df.join(transaction_df, user_df.user_id == transaction_df.userid)
    return total_df
def UniqueLocation(total_df):
    # a): Count of unique locations where each product is sold
    product_locations_df = total_df.groupBy("product_id").agg(countDistinct("location ").alias("UniqueLocations"))
    return product_locations_df
def ProductBought(total_df):
    # b): Find out products bought by each user
    user_products_df = total_df.groupBy("userid").agg(collect_list("product_id").alias("ProductsBought"))
    return user_products_df
def TotalSpending(total_df):
    # c): Total spending done by each user on each product
    user_product_spending_df = total_df.groupBy("userid", "product_id").agg(sum("price").alias("TotalSpending"))
    return user_product_spending_df


