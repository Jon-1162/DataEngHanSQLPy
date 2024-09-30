# Problem # 3 - - - - - - - - - - - - - - - - - - - - - - - - - - - 
# DELETE leftTable FROM transactions leftTable 
#   INNER JOIN 
#       transactions rightTable WHERE 
#           leftTable.id > rightTable.id AND 
#           leftTable.user_id= rightTable.user_id AND
#           leftTable.transaction_date= rightTable.transaction_date;
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

#  Setup for problems 4 and 5
import pymysql
import pandas as pd

conn = pymysql.connect(
    host='127.0.0.1',        
    user='root',    
    password='pw', 
    database='LocalTestHanover', 
)

cursor = conn.cursor()


# Problem-4 - - - - - - - - - - - - - - - - - - - - - - - - - - - 
# SQL Query - - - - -

# SELECT region, sum(active) AS active_users FROM users GROUP BY region;

# Python - - - - -

# This question didn't specify how to do this question in python so I 
# have 2 approaches to do this in python. One using just a DB connection.
# With another approach using pandas.

# Just using a SQL connection - - - - 

cursor.execute("SELECT region, sum(active) AS active_users FROM users GROUP BY region;")

# Ordered data with pandas - - - -

df = pd.read_sql("SELECT * FROM users", con=conn)


p4df = df.groupby('region')['active'].agg(active_users='sum').reindex()

print(p4df)

# Partitioning strategy suggestion  - - - - 
# To optimize queries based on region a separate table could be made with related aggregates
# that is updated either on 'users' table update or updated on increments of time like once a day

# Problem-5 - - - - - - - - - - - - - - - - - - - - - - - - - - - 
dfEmployees = pd.read_sql("SELECT * FROM Employees", con=conn)
dfDepartments = pd.read_sql("SELECT * FROM Departments", con=conn)

#  5.1 - - - -
# SQL

# SELECT Employees.EmployeeID, Employees.FirstName, Employees.LastName, Departments.DepartmentName
# FROM Employees
# INNER JOIN Departments ON Departments.DepartmentID=Employees.DepartmentID;

# Python approaches
# sql connection execution
cursor.execute('SELECT Employees.EmployeeID, Employees.FirstName, Employees.LastName, Departments.DepartmentName FROM Employees INNER JOIN Departments ON Departments.DepartmentID=Employees.DepartmentID;')

# pandas
dfEmployeesWDpt = pd.merge(dfDepartments, dfEmployees, on='DepartmentID', how='inner').drop(['DepartmentID'
,'Salary'], axis=1, inplace=False)[['EmployeeID','FirstName','LastName','DepartmentName' ]].sort_values(by=['EmployeeID'])

print(dfEmployeesWDpt)


#  5.2 - - - -
# SQL
# SELECT Departments.DepartmentName, IFNULL((SELECT SUM(Salary) FROM Employees WHERE Employees.DepartmentID = Departments.DepartmentID), 0) AS TotalSalary FROM Departments;
# Python

# sql connection execution
cursor.execute('SELECT Departments.DepartmentName, IFNULL((SELECT SUM(Salary) FROM Employees WHERE Employees.DepartmentID = Departments.DepartmentID), 0) AS TotalSalary FROM Departments;')

# pandas
# Used chat gpt to learn about how to aggregate in dfs
# Used chat gpt to learn about inplace argument
dfDptsTotalSalary = pd.merge(dfDepartments, dfEmployees, on='DepartmentID', how='inner').groupby('DepartmentName').sum().drop(['DepartmentID','EmployeeID', 'FirstName', 'LastName'], axis=1, inplace=False).reset_index().sort_values(by=['Salary'])
print(dfDptsTotalSalary.rename(columns={"DepartmentName": "DepartmentName", "Salary": "TotalSalary"}, inplace=False))

#  5.3 - - - -
# SQL
# SELECT DepartmentName FROM Departments INNER JOIN Employees ON Employees.DepartmentID= Departments.DepartmentID GROUP BY DepartmentName HAVING Count(*) > 1;

# Python
# sql connection execution
cursor.execute('SELECT DepartmentName FROM Departments INNER JOIN Employees ON Employees.DepartmentID= Departments.DepartmentID GROUP BY DepartmentName HAVING Count(*) > 1;')
# pandas

dfBigDpts = pd.merge(dfDepartments, dfEmployees, on='DepartmentID', how='inner').drop(['DepartmentID','EmployeeID','FirstName','LastName','Salary'],axis=1, inplace=False)
dfBigDpts = dfBigDpts[dfBigDpts.groupby('DepartmentName').transform('size')>1].drop_duplicates()
print(dfBigDpts)
conn.close()