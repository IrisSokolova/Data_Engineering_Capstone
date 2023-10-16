## Data_Engineering_Capstone
ETL process for a Loan Application dataset and a Credit Card dataset

### Credit Card System Data Engineering Capstone

This capstone project showcases the application of various technologies to manage an ETL (Extract, Transform, Load) process for a Loan Application dataset and a Credit Card dataset. The project involves utilizing Python (Pandas, advanced modules like Matplotlib), SQL, Apache Spark (Spark Core, Spark SQL), and Python Visualization and Analytics libraries.

### Project Overview

The main objective of this project is to work with diverse technologies in order to handle ETL processes for two critical datasets: Loan Application dataset and Credit Card dataset. These datasets provide vital information related to loan applications, customer transactions, and inventory management within the credit card system.

### Dataset Details

The project involves working with the following dataset files:

1. `CDW_SAPP_CUSTOMER.JSON`: Contains existing customer details.
2. `CDW_SAPP_CREDITCARD.JSON`: Provides credit card transaction information.
3. `CDW_SAPP_BRANCH.JSON`: Contains detailed information about each branch.

### Extract Transform Load processes 

This script demonstrates ETL processes by transforming and loading data into a MySQL database, including loading data from an API. It also highlights the use of Spark for data processing and integration with MySQL for storing transformed data.
1. Data Transformation and Cleaning:
	- Transforms and cleans three DataFrames ('customer', 'branch', 'credit') using various operations like column type casting, string formatting, concatenation, and dropping unnecessary columns.
2. Database Setup:
	- Establishes a connection to a MySQL database using credentials from a file.
	- Creates a new database if it doesn't exist and sets it as the active database.
3. Writing to Database:
	- Writes the transformed DataFrames to the MySQL database using JDBC with specified table names.
4. Data Retrieval from API:
	- Retrieves data from a given API endpoint representing loan application data.
5. Spark Session Setup:
	- Sets up a Spark session with specified configurations, including the JDBC connector.

### Application Front-End

Once the data is successfully loaded into the database, a console-based Python program is designed to cater to the following system requirements: 

Customers mode:  
```
  Menu:
      1. Check existing account details of a customer
      2. Modify existing account details of a customer
      3. Generate a monthly bill for a credit card number
      4. Display transactions made by a customer between two dates
      5. Exit
```
Transactional mode:
```
    Menu:
    1. Distplay the transactions made by customers living in a given zip code for a given month and year
    2. Display the number and total values of transactions for a given type
    3. Display the total number and total values of transactions for branches in a given state?
    4. Exit
```
 
### Data Analysis and Visualization

After the data is loaded into the database and can be accessed via the front end, the project addresses the business analyst team's need for data analysis and visualization using Python libraries. The focus is on analyzing loan application data and automating the loan eligibility process based on customer details.
```
   Menu:
    1. Plot transaction type with the highest transaction count
    2. Plot state with a high number of customers
    3. Plot sum of all transactions for the top 10 customers
    4. Plot percentage of applications approved for self-employed applicants
    5. Plot percentage of rejection for married male applicants
    6. Plot the top three months with the largest volume of transaction data
    7. Plot which branch processed the highest total dollar value of healthcare transactions

```

### Data Sources

The project utilizes an external API endpoint to access loan application data. The API provides a dataset with essential fields required for loan application analysis.

- **API Endpoint**: [Loan Application Data API](https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json)

### Project Files

- **`DB_Create_Transform.py`**: Script for database creation and data transformation.
- **`Customers_mode.py`**: Module for managing customer-related functionalities.
- **`Transactional_mode.py`**: Module for handling transactional data.
- **`Bank_visulization.py`**: Module for visualizing banking data.
- **`Visualization` Folder**: Contains files related to visualization functionalities.

## How to Run

1. Ensure all necessary libraries and dependencies are installed.
2. Run the scripts and modules as needed for data processing, transformation, and visualization.

Feel free to explore and adapt the project based on specific requirements and use cases.