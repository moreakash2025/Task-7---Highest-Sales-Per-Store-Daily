# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, rank
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName("HighestSalePerStoreDaily").getOrCreate()
salesOrderDetailDF = spark.read.format("csv").options(header=True).load("/FileStore/tables/Sales_SalesOrderDetail.csv")
salesOrderDetailDF.show()

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType

schema = StructType([
    StructField("SalesOrderID", IntegerType(), True),
    StructField("SalesOrderDetailID", IntegerType(), True),
    StructField("CarrierTrackingNumber", StringType(), True),
    StructField("OrderQty", IntegerType(), True),
    StructField("ProductID", IntegerType(), True),
    StructField("SpecialOfferID", IntegerType(), True),
    StructField("UnitPrice", DoubleType(), True),
    StructField("UnitPriceDiscount", DoubleType(), True),
    StructField("LineTotal", DoubleType(), True),
    StructField("rowguid", StringType(), True),
    StructField("ModifiedDate", TimestampType(), True)
])

salesOrderDetailDF = spark.read.format("csv").schema(schema).load("/FileStore/tables/Sales_SalesOrderDetail.csv")
salesOrderDetailDF.show()

# Load the SalesOrderHeader DataFrame
salesOrderHeaderDF = spark.read.format("csv").option("header", True).load("/FileStore/tables/Sales_SalesOrderHeader.csv")
salesOrderHeaderDF.show()

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType

schema = StructType([
    StructField("SalesOrderID", IntegerType(), True),
    StructField("RevisionNumber", IntegerType(), True),
    StructField("OrderDate", TimestampType(), True),
    StructField("DueDate", TimestampType(), True),
    StructField("ShipDate", TimestampType(), True),
    StructField("Status", IntegerType(), True),
    StructField("OnlineOrderFlag", IntegerType(), True),
    StructField("SalesOrderNumber", StringType(), True),
    StructField("PurchaseOrderNumber", StringType(), True),
    StructField("AccountNumber", StringType(), True),
    StructField("CustomerID", IntegerType(), True),
    StructField("SalesPersonID", IntegerType(), True),
    StructField("TerritoryID", IntegerType(), True),
    StructField("BillToAddressID", IntegerType(), True),
    StructField("ShipToAddressID", IntegerType(), True),
    StructField("ShipMethodID", IntegerType(), True),
    StructField("CreditCardID", IntegerType(), True),
    StructField("CreditCardApprovalCode", StringType(), True),
    StructField("CurrencyRateID", IntegerType(), True),
    StructField("SubTotal", DoubleType(), True),
    StructField("TaxAmt", DoubleType(), True),
    StructField("Freight", DoubleType(), True),
    StructField("TotalDue", DoubleType(), True),
    StructField("Comment", StringType(), True),
    StructField("rowguid", StringType(), True),
])

# Example of reading the CSV file with the schema
salesOrderHeaderDF = spark.read.format("csv").schema(schema).load("/FileStore/tables/Sales_SalesOrderHeader.csv")
salesOrderHeaderDF.show()
sales_df = salesOrderDetailDF.join(salesOrderHeaderDF, "SalesOrderID")

# Calculate the total sales per store per day
daily_sales_df = sales_df.groupBy(
    "SalesOrderID", "ModifiedDate"
).agg(
    sum("LineTotal").alias("TotalSales")
)

# Define a window partitioned by StoreID and ModifiedDate and ordered by TotalSales descending
window_spec = Window.partitionBy("SalesOrderID", "ModifiedDate").orderBy(daily_sales_df["TotalSales"].desc())

ranked_sales_df = daily_sales_df.withColumn("SalesRank", rank().over(window_spec))
highest_sales_df = ranked_sales_df.filter(ranked_sales_df["SalesRank"] == 1)
highest_sales_df.orderBy("SalesOrderID", "ModifiedDate").show()

# COMMAND ----------

/FileStore/tables/Sales_SalesOrderDetail.csv

# COMMAND ----------

/FileStore/tables/Sales_SalesOrderHeader.csv
