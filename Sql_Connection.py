import mysql.connector as sqlcon
import pandas as pd
import sqlalchemy as sa
con=sqlcon.connect(user='root',host='localhost',passwd='mysql',database="hindalco")
if con.is_connected():   # Checking if the Connection Established or not
    print("connection is established")
cursor=con.cursor()     # creating a cursor
cursor.execute("Drop table if exists hindalco")
#Creating a table
Create_Table=("Create Table if not exists HINDALCO (Index_no int not null primary key,\
                                            Date_Time varchar(50) not null,\
                                            Close decimal,\
                                            High decimal,\
                                            Low decimal,\
                                            Open decimal,\
                                            Volume int,\
                                            Instrument char(8))")
cursor.execute(Create_Table)  # execute the command

# Inserting values in the table
Insert_Row=("Insert into HINDALCO values(%s,%s,%s,%s,%s,%s,%s,%s)")

#reading csv file to export data in database
df=pd.read_csv(r"D://Intershala MusicPerk//HindalCo//HINDALCO_1D.csv")

# Exporting Values in the mysql database
for (index,row_series) in df.iterrows():
    Row_value=list(row_series.values)
    Row_value.insert(0,index)
    cursor.execute(Insert_Row,Row_value)
   
con.commit() # Committing changes

print(len(Row_value))

# Fetching 20 rows from database table
query="SELECT * from HINDALCO limit 20"
cursor.execute(query)
for row in cursor.fetchall():
    print(row)

cursor.close() 
con.close()  # closing mysql connection