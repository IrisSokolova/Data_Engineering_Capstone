import os
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext 
import mysql.connector

spark = SparkSession.builder \
    .appName("PySpark MySQL Connection") \
    .config("spark.jars", "./mysql-connector-java-8.0.23.jar") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

    
print("___________________________________________________________")  
print('Spark session created.')
print("___________________________________________________________")  

# Set the JDBC URL and table name
url='jdbc:mysql://localhost:3306/creditcard_capstone'

# Path to your MySQL JDBC connector JAR
mysql_jar_path = "./mysql-connector-java-8.0.23.jar"

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

# Set the properties including user and password
properties = {
    "user": mylogin,
    "password": mypass,
    "driver": "com.mysql.jdbc.Driver"
}

customer_mySQL = 'creditcard_capstone.CDW_SAPP_CUSTOMER'
credit_mySQL = 'creditcard_capstone.CDW_SAPP_CREDIT_CARD'
branch_mySQL = 'creditcard_capstone.CDW_SAPP_BRANCH'

df_customer = spark.read.jdbc(url, customer_mySQL, properties=properties)
df_credit = spark.read.jdbc(url, credit_mySQL, properties=properties)
df_branch = spark.read.jdbc(url, branch_mySQL, properties=properties)

df_customer.createOrReplaceTempView('customer')
df_credit.createOrReplaceTempView('credit')
df_branch.createOrReplaceTempView('branch')

print("___________________________________________________________")
print('Data is ready for reports')
print("___________________________________________________________")

    
def transaction_by_customers_zip():
    zip = input('Enter zip(6 digit numbers): ')
    while len(zip)  != 5 and str.isdigit(zip):
            print('Not correct zip code!')
            zip = input('Enter zip(6 digit numbers): ')
    year = input('Enter Year(4 digit numbers): ')
    while len(year) !=4 and str.isdigit(year):
            print('Not correct year!')
            year = input('Enter year(4 digit numbers): ')
    month = input('Enter Month(two digit numbers): ')
    while len(month) !=2 and str.isdigit(month):
            print('Not correct month! ')
            month = input('Enter Month(two digit numbers): ')
    query_transaction_zip = f"select TRANSACTION_ID, \
                            TRANSACTION_VALUE, \
                            TRANSACTION_TYPE, \
                            TIMEID, \
                            FIRST_NAME, \
                            LAST_NAME, \
                            CUST_ZIP, \
                            YEAR, \
                            MONTH \
                            from credit t \
                            left join customer c \
                            on t.CUST_SSN = c.SSN \
                            where c.cust_zip = {zip} \
                            and t.YEAR = {year} and t.MONTH = {month}"
    result_transaction_by_zip = spark.sql(query_transaction_zip) 
    print(result_transaction_by_zip.show())
    
def transaction_by_type():
         print("List of transactions: ")
         trans_type = spark.sql('Select distinct TRANSACTION_TYPE from credit')
         # Collect the distinct TRANSACTION_TYPE values into a list
         trans_type_list = trans_type.rdd.map(lambda row: row[0]).collect()
         print(trans_type.show())
         transaction_type = input('Enter transaction type one of the listed above: ')
         while transaction_type not in trans_type_list:
             print('Not correct Transaction type!')
             transaction_type = input('Enter transaction type: ')
         query_total_by_type = f"select TRANSACTION_TYPE, round(sum(TRANSACTION_VALUE), 2) as total_value \
                                from credit \
                                where TRANSACTION_TYPE = '{transaction_type}' \
                                group by TRANSACTION_TYPE"
         result_total_by_type = spark.sql(query_total_by_type)
         print(result_total_by_type.show())
         
def total_count_sum_transaction_branch_state():
            branch_name = spark.sql('select distinct BRANCH_STATE from branch')
            print(branch_name.show())
            branch_list = branch_name.rdd.map(lambda row: row[0]).collect()
            state = input('Enter one of the State literals as shown above: ')
            while state not in branch_list:
                print('Not correct State literal!')
                state = input('Enter one of the State literals: ')
            query_state = f"select count(t.TRANSACTION_ID), \
                            round(sum(t.TRANSACTION_VALUE), 2) \
                            from credit t \
                            left join branch b \
                            on t.BRANCH_CODE = b.BRANCH_CODE \
                            where b.BRANCH_STATE = '{state}'"
            result_total_count_sum_trans_by_state = spark.sql(query_state)
            print(result_total_count_sum_trans_by_state.show())

# clear screen
os.system('cls' if os.name == 'nt' else 'clear')

# Menu for user interaction
while True:
    print("Menu:")
    print("1. Distplay the transactions made by customers living in a given zip code for a given month and year")
    print("2. Display the number and total values of transactions for a given type")
    print("3. Display the total number and total values of transactions for branches in a given state?")
    print("4. Exit")

    choice = input("Enter your choice: ")

    if choice == '1':
        transaction_by_customers_zip()
    elif choice == '2':
        transaction_by_type()
    elif choice == '3':
        total_count_sum_transaction_branch_state()
    elif choice == '4':
        break
    else:
        print("Invalid choice. Please try again.")
            
            
print('Thank you! You are reach the end of transaction mode!')

spark.stop()