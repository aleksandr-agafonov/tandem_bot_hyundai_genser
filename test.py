import pyodbc


server = 'prsunvsu17.database.windows.net'
database = 'mybi-hpnqdjf'
username = 'owner-hpnqdjf'
password = 'VxBvU9dYXyyr'

cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()