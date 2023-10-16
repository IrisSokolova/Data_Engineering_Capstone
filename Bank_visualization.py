import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql import SparkSession

import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext 
from pyspark.sql.functions import when, col, count, sum
from pyspark.sql.types import DoubleType

import matplotlib.pyplot as plt
import pandas as pd


# Initialize a Spark session
spark = SparkSession.builder \
    .appName("PySpark MySQL Connection") \
    .config("spark.jars", "./mysql-connector-java-8.0.23.jar") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()
    
# Set the JDBC URL and table name
url='jdbc:mysql://localhost:3306/creditcard_capstone'

# Path to your MySQL JDBC connector JAR
mysql_jar_path = "./mysql-connector-java-8.0.23.jar"

print("___________________________________________________________")
print('Spark session created.')

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
loan_mySQL = 'creditcard_capstone.CDW_SAPP_loan_application'

# Load dataframes
df_customer = spark.read.jdbc(url, customer_mySQL, properties=properties)
df_credit = spark.read.jdbc(url, credit_mySQL, properties=properties)
df_branch = spark.read.jdbc(url, branch_mySQL, properties=properties)
df_loan = spark.read.jdbc(url, loan_mySQL, properties=properties)

print("___________________________________________________________")
print("Data is ready")
print("___________________________________________________________")


def plot_transaction_type_with_highest_count():
    # Group by transaction type and count transactions for each type
    transaction_counts = df_credit.groupBy('TRANSACTION_TYPE').count()

    # Find the transaction type with the highest transaction count
    max_transaction_count = transaction_counts.orderBy('count', ascending=False).first()

    # Plot the transaction counts for each type
    transaction_counts_pd = transaction_counts.toPandas()
    transaction_counts_pd.plot(kind='bar', x='TRANSACTION_TYPE', y='count', 
                               title='Transaction Type vs Transaction Count')
    plt.show()
    
    # Plot the transaction counts for each type using a pie chart
    plt.figure(figsize=(10, 6))
    plt.pie(transaction_counts_pd['count'], labels=transaction_counts_pd['TRANSACTION_TYPE'], autopct='%1.1f%%')
    plt.title('Transaction Type Distribution')
    plt.show()

    print('Transaction type with the highest transaction count:')
    print(max_transaction_count)

def plot_state_with_highest_customer_count():
    # Group by state and count customers for each state
    customer_counts_by_state = df_customer.groupBy('CUST_STATE').count()

    # Find the state with the highest number of customers
    max_customer_count_state = customer_counts_by_state.orderBy('count', ascending=False).first()

    # Plot the number of customers for each state
    customer_counts_by_state_pd = customer_counts_by_state.toPandas()
    customer_counts_by_state_pd.plot(kind='bar', x='CUST_STATE', y='count', 
                                     title='State vs Customer Count')
    plt.show()

    print('State with the highest number of customers:')
    print(max_customer_count_state)

def plot_sum_of_transactions_for_top_10_customers():
    # Group by customer SSN and sum the transaction values for each customer
    customer_transaction_sum = df_credit.groupBy('CUST_SSN').sum('TRANSACTION_VALUE')

    # Sort by transaction sum in descending order
    customer_transaction_sum = customer_transaction_sum.orderBy('sum(TRANSACTION_VALUE)', ascending=False)

    # Select top 10 customers
    top_10_customers = customer_transaction_sum.limit(10)

    # Plot the sum of transactions for the top 10 customers
    top_10_customers_pd = top_10_customers.toPandas()
    top_10_customers_pd.plot(kind='bar', x='CUST_SSN', y='sum(TRANSACTION_VALUE)', 
                              title='Top 10 Customers: Sum of Transactions')
    plt.show()

    # Find the customer with the highest transaction amount
    customer_highest_transaction = customer_transaction_sum.orderBy('sum(TRANSACTION_VALUE)', 
                                                                     ascending=False).first()

    print('Customer with the highest transaction amount:')
    print(customer_highest_transaction)
    
def plot_self_employed_approval_percentage():
    # Calculate the percentage of applications approved for self-employed applicants
    self_employed_approval_percentage = df_loan.filter(col("Self_Employed") == "Yes") \
        .groupBy("Application_Status") \
        .count() \
        .withColumn("Percentage", (col("count") / df_loan.count()) * 100) \
        .toPandas()
    # Plot the percentage of applications approved for self-employed applicants
    plt.figure(figsize=(6, 6))
    plt.pie(self_employed_approval_percentage['Percentage'], labels=self_employed_approval_percentage['Application_Status'], autopct='%1.1f%%')
    plt.title('Percentage of Applications Approved for Self-Employed Applicants')
    plt.show()

def plot_rejection_percentage_married_male():
    # Calculate the percentage of rejection for married male applicants
    rejection_percentage_married_male = df_loan.filter((col("Married") == "Yes") & (col("Gender") == "Male") & (col("Application_Status") == "N")) \
        .count()

    total_married_male_applicants = df_loan.filter((col("Married") == "Yes") & (col("Gender") == "Male")) \
        .count()

    rejection_percentage = (rejection_percentage_married_male / total_married_male_applicants) * 100

    print("Percentage of rejection for married male applicants:", rejection_percentage)

    # Plot the percentage of rejection for married male applicants
    labels = ['Rejection', 'Approval']
    sizes = [rejection_percentage, 100 - rejection_percentage]

    plt.figure(figsize=(6, 6))
    plt.pie(sizes, labels=labels, autopct='%1.1f%%')
    plt.title('Percentage of Rejection for Married Male Applicants')
    plt.show()

def plot_top_three_transaction_months():
    
    top_three_months = df_credit.groupBy("MONTH").agg(count("TRANSACTION_ID").alias("transaction_count")) \
        .orderBy("transaction_count", ascending=False).limit(3).toPandas()
    # Plot the top three months
    plt.bar(top_three_months['MONTH'], top_three_months["transaction_count"])
    plt.xlabel('Month')
    plt.ylabel('Volume')
    plt.title('Top Three Months with Largest Volume of Transaction Data')
    plt.show()

# Cast "TRANSACTION_VALUE" to double and then perform the aggregation
def plot_highest_dollar_value_branch_healthcare():
    highest_dollar_value_branch = df_credit \
        .filter(col("TRANSACTION_TYPE") == "Healthcare") \
        .withColumn("TRANSACTION_VALUE", df_credit["TRANSACTION_VALUE"].cast(DoubleType())) \
        .groupBy("BRANCH_CODE") \
        .agg(sum("TRANSACTION_VALUE").alias("dollar_value")) \
        .orderBy(col("dollar_value").desc()) \
        .first()
    
    dollar_value_branch = df_credit \
        .filter(col("TRANSACTION_TYPE") == "Healthcare") \
        .withColumn("TRANSACTION_VALUE", df_credit["TRANSACTION_VALUE"].cast(DoubleType())) \
        .groupBy("BRANCH_CODE") \
        .agg(sum("TRANSACTION_VALUE").alias("dollar_value")) \
        .orderBy(col("dollar_value").desc()).limit(10).toPandas()
    # Plot the data using Matplotlib
    # Extract values
    # Find the branch_code with the highest dollar_value
    highest_dollar_value = dollar_value_branch['dollar_value'].max()
    highest_dollar_value_branch = dollar_value_branch[dollar_value_branch['dollar_value'] == highest_dollar_value]

    # Plot the data using Matplotlib scatter plot
    plt.figure(figsize=(10, 6))

    # Plot all points in blue
    plt.scatter(dollar_value_branch['BRANCH_CODE'], dollar_value_branch['dollar_value'], color='blue', s=dollar_value_branch['dollar_value']*0.3)  

    # Plot the highest value in red
    plt.scatter(highest_dollar_value_branch['BRANCH_CODE'], highest_dollar_value_branch['dollar_value'], color='red', s=highest_dollar_value_branch['dollar_value']*1)  

    plt.xlabel('Branch Code')
    plt.ylabel('Dollar Value')
    plt.title('Top 10 Branches by Dollar Value for Healthcare Transactions')

    # Annotate each point with branch_code and dollar_value
    for i, txt in enumerate(dollar_value_branch['BRANCH_CODE']):
        plt.annotate(f'Branch: {txt}\nDollar Value: {dollar_value_branch["dollar_value"].iloc[i]:.2f}', 
                    (dollar_value_branch['BRANCH_CODE'].iloc[i], dollar_value_branch['dollar_value'].iloc[i]), 
                    textcoords="offset points", 
                    xytext=(0,10), 
                    ha='center')

    plt.show()
    # Extract values
    branch_code = highest_dollar_value_branch.BRANCH_CODE
    dollar_value = highest_dollar_value_branch.dollar_value.apply(lambda x: f"{x:.2f}")

    # Print in a formatted way
    print(f"Branch Code: {branch_code}")
    print(f"Dollar Value: {dollar_value}")
       
# Main menu
while True:
    print("___________________________________________________________")
    print("Menu:")
    print("___________________________________________________________")
    print("1. Plot transaction type with the highest transaction count")
    print("2. Plot state with a high number of customers")
    print("3. Plot sum of all transactions for the top 10 customers")
    print("4. Plot percentage of applications approved for self-employed applicants")
    print("5. Plot percentage of rejection for married male applicants")
    print("6. Plot the top three months with the largest volume of transaction data")
    print("7. Plot which branch processed the highest total dollar value of healthcare transactions")
    print("___________________________________________________________")
    print("8. Exit")

    choice = input("Enter your choice: ")

    if choice == '1':
        plot_transaction_type_with_highest_count()
    elif choice == '2':
        plot_state_with_highest_customer_count()
    elif choice == '3':
        plot_sum_of_transactions_for_top_10_customers()
    elif choice == '4':
        plot_self_employed_approval_percentage()
    elif choice == '5':
        plot_rejection_percentage_married_male()
    elif choice == '6':
        plot_top_three_transaction_months()
    elif choice == '7':
        plot_highest_dollar_value_branch_healthcare()
    elif choice == '8':
        break
    else:
        print("Invalid choice. Please select a valid option.")

# Stop the Spark session
spark.stop()