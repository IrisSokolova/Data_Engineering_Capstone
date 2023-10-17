import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext 

spark = SparkSession.builder \
    .appName("PySpark MySQL Connection") \
    .config("spark.jars", "./mysql-connector-java-8.0.23.jar") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()
# Set the JDBC URL and table name
url='jdbc:mysql://localhost:3306/creditcard_capstone'

# Path to your MySQL JDBC connector JAR
mysql_jar_path = "./mysql-connector-java-8.0.23.jar"

print("________________________________")
print('Spark session created.')

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

db_connection = mysql.connector.connect(user=mylogin, password=mypass)
db_cursor = db_connection.cursor()
db_cursor.execute("USE creditcard_capstone;")

print("________________________________")
print("Data is ready for Transformation.")

# 1) Used to check the existing account details of a customer.
# 2) Used to modify the existing account details of a customer.
# 3) Used to generate a monthly bill for a credit card number for a given month and year.
# 4) Used to display the transactions made by a customer between two dates. Order by year, month, and day in descending order.

# Action 1: Check existing account details of a customer
def check_account_details():
    cust_ssn = input("Enter customer SSN: ")
    query = f"SELECT * FROM customer WHERE SSN = {cust_ssn}"
    result = spark.sql(query)
    result.show()

# Action 2: Modify existing account details of a customer
   
def modify_account_details():
    cust_ssn = input("Enter customer SSN: ")
    
    # Dictionary to map user's choice to the corresponding column name
    columns = {
        '1': 'CREDIT_CARD_NO',
        '2': 'CUST_CITY',
        '3': 'CUST_COUNTRY',
        '4': 'CUST_EMAIL',
        '5': 'CUST_PHONE',
        '6': 'CUST_STATE',
        '7': 'CUST_ZIP',
        '8': 'FIRST_NAME',
        '9': 'LAST_NAME',
        '10': 'LAST_UPDATED',
        '11': 'MIDDLE_NAME',
        '12': 'FULL_STREET_ADDRESS'
    }
    
    # Display the menu for column selection
  
    print("Select a column to modify:")
    for key, value in columns.items():
        print(f"{key}. {value}")
    
    choice = input("Enter your choice: ")
    
    # Check if the choice is valid
    if choice not in columns:
        print("Invalid choice. Please select a valid option.")
        return
    
    new_value = input(f"Enter the new value for {columns[choice]}: ")
    
   # Update the specified column for the customer
   # query = f"UPDATE customer SET {columns[choice]} = '{new_value}' WHERE SSN = {cust_ssn}"
    
    # Execute the SQL query to modify the account details
    db_cursor.execute("USE creditcard_capstone;")
    update_query = f"UPDATE cdw_sapp_customer SET {columns[choice]} = '{new_value}' WHERE SSN = {cust_ssn};"
    db_cursor.execute(update_query)
    db_connection.commit()
    # spark.sql(query)
    
    print("Account details modified successfully.")

# Action 3: Generate a monthly bill for a credit card number for a given month and year
def generate_monthly_bill():
    credit_card_no = input("Enter credit card number: ")
    while len(credit_card_no) != 16 and str.isdigit(credit_card_no):
        print('Not valid number!')
    year = input("Enter year: ")
    while len(year) !=4 and str.isdigit(year):
        print('Not correct year!')
    month = input("Enter month: ")
    while len(month) !=2 and str.isdigit(month):
        print('Not correct month!')
    query = f"""
        SELECT TRANSACTION_ID, TRANSACTION_TYPE, TRANSACTION_VALUE
        FROM credit
        WHERE CUST_CC_NO = {credit_card_no}
            AND YEAR = {year}
            AND MONTH = {month}
    """
    query_total = f"""SELECT SUM(TRANSACTION_VALUE) as Total_value
                FROM credit
                WHERE CUST_CC_NO = {credit_card_no}
                AND YEAR = {year}
                AND MONTH = {month}
                """
    result = spark.sql(query)
    result_total = spark.sql(query_total)
    result.show()
    result_total.show()

# Action 4: Display transactions made by a customer between two dates
def display_transactions_between_dates():
    cust_ssn = input("Enter customer SSN: ")
    start_date = input("Enter start date (YYYYMMDD): ")
    end_date = input("Enter end date (YYYYMMDD): ")
    query = f"""
        SELECT *
        FROM credit
        WHERE CUST_SSN = {cust_ssn}
            AND TIMEID >= {start_date}
            AND TIMEID <= {end_date}
        ORDER BY YEAR DESC, MONTH DESC, DAY DESC
    """
    result = spark.sql(query)
    result.show()
 
 # clear screen
os.system('cls' if os.name == 'nt' else 'clear')
    
# Menu for user interaction
while True:
    print("Menu:")
    print("1. Check existing account details of a customer")
    print("2. Modify existing account details of a customer")
    print("3. Generate a monthly bill for a credit card number")
    print("4. Display transactions made by a customer between two dates")
    print("5. Exit")

    choice = input("Enter your choice: ")

    if choice == '1':
        check_account_details()
    elif choice == '2':
        modify_account_details()
    elif choice == '3':
        generate_monthly_bill()
    elif choice == '4':
        display_transactions_between_dates()
    elif choice == '5':
        break
    else:
        print("Invalid choice. Please try again.")

# Stop the Spark session
spark.stop()