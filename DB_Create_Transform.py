import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext 

from pyspark.sql.functions import format_string, substring, lpad, concat, initcap, lower, concat_ws, col, concat, lit
from pyspark.sql.types import IntegerType, TimestampType, StringType,  DoubleType

spark = SparkSession.builder \
    .appName("PySpark MySQL Connection") \
    .config("spark.jars", "/Users/iris/Downloads/mysql-connector-java-8.0.23.jar") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

path_customer = './data/cdw_sapp_custmer.json'
path_credit = './data/cdw_sapp_credit.json'
path_branch = './data/cdw_sapp_branch.json'
loan_endpoint = 'https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json'

branch = spark.read.json(path_branch)
credit = spark.read.json(path_credit)
customer = spark.read.json(path_customer)                                           

# initcap
# lower
# format_strin

customer_df = customer \
              .withColumn("FIRST_NAME",
                          initcap(customer["FIRST_NAME"])) \
              .withColumn("MIDDLE_NAME",
                          lower(customer["MIDDLE_NAME"])) \
              .withColumn("LAST_NAME",
                          initcap(customer["LAST_NAME"])) \
              .withColumn("CUST_PHONE",
                          format_string("(000)%s-%s", substring("CUST_PHONE", 1, 3), substring("CUST_PHONE", 4, 7))) \
              .withColumn('FULL_STREET_ADDRESS', 
                          concat(customer['APT_NO'], lit(','), customer['STREET_NAME'])) \
              .withColumn('SSN', 
                          customer['SSN'].cast(IntegerType())) \
              .withColumn('CUST_ZIP', 
                          customer['CUST_ZIP'].cast(IntegerType())) \
              .withColumn('LAST_UPDATED', 
                          customer['LAST_UPDATED'].cast(TimestampType()))

columns_to_drop = ['STREET_NAME','APT_NO']
customer_df = customer_df.drop(*columns_to_drop)

branch_df = branch \
            .withColumn("BRANCH_CODE",
                        branch["BRANCH_CODE"].cast(IntegerType())) \
            .withColumn("BRANCH_ZIP",
                        branch["BRANCH_ZIP"].cast(IntegerType())) \
            .withColumn("LAST_UPDATED",
                        branch["LAST_UPDATED"].cast(TimestampType())) \
            .withColumn("BRANCH_PHONE",
                        format_string("(%s)%s-%s", substring("BRANCH_PHONE", 1, 3), substring("BRANCH_PHONE", 4, 3), substring("BRANCH_PHONE", 7, 4)))
branch_df.na.fill(value=99999, subset=["BRANCH_ZIP"])

credit_df = credit \
            .withColumn("CUST_CC_NO",
                        credit["CREDIT_CARD_NO"]) \
            .withColumn("CUST_SSN",
                        credit["CUST_SSN"].cast(IntegerType())) \
            .withColumn("DAY",
                        credit["DAY"].cast(StringType())) \
            .withColumn("MONTH",
                        credit["MONTH"].cast(StringType())) \
            .withColumn("YEAR",
                        credit["YEAR"].cast(StringType())) \
            .withColumn("BRANCH_CODE",
                        credit["BRANCH_CODE"].cast(IntegerType())) \
            .withColumn("TRANSACTION_ID",
                        credit["TRANSACTION_ID"].cast(IntegerType())) \
            .withColumn("TRANSACTION_VALUE",
                        credit["TRANSACTION_VALUE"].cast(DoubleType()))
            
credit_df = credit_df \
            .withColumn("DAY",
                        lpad(credit_df["DAY"], 2, "0")) \
            .withColumn("MONTH",
                        lpad(credit_df["MONTH"], 2, "0"))

credit_df = credit_df.withColumn("TIMEID", concat(credit_df['YEAR'], credit_df['MONTH'], credit_df['DAY']))

columns_to_drop = ['CREDIT_CARD_NO']
credit_df = credit_df.drop(*columns_to_drop)
credit_df = credit_df.withColumn("TIMEID", credit_df["TIMEID"].cast(StringType()))

# creditcard_capstone database
import mysql.connector
import os 
file_path = "./secret.txt"  # Replace with the actual path

# Read the file and extract credentials
credentials = {}
with open(file_path, 'r') as file:
    for line in file:
        key, value = line.strip().split(': ')
        credentials[key] = value

# Access the credentials
mypass = credentials.get('MYPASS')
mylogin = credentials.get('MYLOGIN')

db_connection = mysql.connector.connect(user=mylogin, password=mypass)
db_cursor = db_connection.cursor()

db_cursor.execute("CREATE DATABASE IF NOT EXISTS creditcard_capstone;")

db_cursor.execute("USE creditcard_capstone;")

# Set the JDBC URL and table name
url='jdbc:mysql://localhost:3306/creditcard_capstone'

# Path to your MySQL JDBC connector JAR
mysql_jar_path = "/Users/iris/Downloads/mysql-connector-java-8.0.23.jar"

# Set the properties including user and password
properties = {
    "user": mylogin,
    "password": mypass,
    "driver": "com.mysql.jdbc.Driver"
}

# Write DataFrame to MySQL
branch_df.write.jdbc(url=url, table="CDW_SAPP_BRANCH", mode="overwrite", properties=properties)
credit_df.write.jdbc(url = url, table="CDW_SAPP_CREDIT_CARD", mode="overwrite", properties=properties)
customer_df.write.jdbc(url = url, table="CDW_SAPP_CUSTOMER", mode="overwrite", properties=properties)


print('______________________________________')
print('DataBase created: creditcard_capstone')
print('_______________________________________')
print('Table CDW_SAPP_BRANCH loaded into MySQL')
print('CDW_SAPP_CREDIT_CARD loaded into MySQL')
print('Table CDW_SAPP_CUSTOMER loaded into MySQL')

spark.stop()