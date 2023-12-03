import pandas as pd
import mysql.connector
import pandas as pd
import os
import json


path1="C:/Users/durga prasad/Desktop/project/pulse/data/aggregated/transaction/country/india/state/"
agg_trans_list=os.listdir(path1)

columns1={'States':[], "Years":[], 'Quarter':[],"Transaction_Type":[],"Transaction_Count":[],"Transaction_Amount":[]}
for state in agg_trans_list:
    cur_states=path1+state+"/"
    agg_year_list=os.listdir(cur_states)


    for year in agg_year_list:
        cur_year=cur_states+year+"/"
        agg_file_list=os.listdir(cur_year)

        for file in agg_file_list:
            cur_file=cur_year+file
            data1=open(cur_file,"r")

            A=json.load(data1)
            for i in A['data']["transactionData"]:
                name=i['name']
                count=i["paymentInstruments"][0]["count"]
                amount=i["paymentInstruments"][0]["amount"]
                columns1["Transaction_Type"].append(name)
                columns1["Transaction_Count"].append(count)
                columns1["Transaction_Amount"].append(amount)
                columns1["States"].append(state)
                columns1["Years"].append(year)
                columns1['Quarter'].append(int(file.strip('.json')))
agg_transaction=pd.DataFrame(columns1)
agg_transaction["States"] = agg_transaction["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
agg_transaction["States"] = agg_transaction["States"].str.replace("-"," ")
agg_transaction["States"] = agg_transaction["States"].str.title()
agg_transaction['States'] = agg_transaction['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")



#agg_user
path2="C:/Users/durga prasad/Desktop/project/pulse/data/aggregated/user/country/india/state/"
columns2={'States':[],'Years':[],'Quarter':[],'Brand':[],"Transaction_Count":[], 'Percentage':[]}
agg_user_list=os.listdir(path2)
for state in agg_user_list:
    cur_states=path2+state+"/"
    agg_year_list=os.listdir(cur_states)

    for year in agg_year_list:
        cur_years=cur_states+year+"/"
        agg_file_list=os.listdir(cur_years)

        for file in agg_file_list:
            cur_file2=cur_years+file
            data=open(cur_file2,"r")

            B=json.load(data)
            try:
                 
                for i in B["data"]["usersByDevice"]:
                        brand=i['brand']
                        count=i['count']
                        percentage=i['percentage']
                        columns2['Brand'].append(brand)
                        columns2['Transaction_Count'].append(count)
                        columns2['Percentage'].append(percentage)
                        columns2["States"].append(state)
                        columns2["Years"].append(year)
                        columns2['Quarter'].append(int(file.strip('.json')))
            except:
                 pass
agg_user=pd.DataFrame(columns2)
agg_user["States"] = agg_user["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
agg_user["States"] = agg_user["States"].str.replace("-"," ")
agg_user["States"] = agg_user["States"].str.title()
agg_user['States'] = agg_user['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")




agg_user
#map_trans

columns3={'States':[],'Years':[],'Quarter':[],'Districts':[],'Transaction_Count':[], "Transaction_Amount":[]}
path3="C:/Users/durga prasad/Desktop/project/pulse/data/map/transaction/hover/country/india/state/"
agg_map_list=os.listdir(path3)
for state in agg_map_list:
    cur_state=path3+state+"/"
    agg_year_list=os.listdir(cur_state)
    for year in agg_year_list:
        cur_year=cur_state+year+"/"
        agg_file_list=os.listdir(cur_year)
        for file in agg_file_list:
            cur_file=cur_year+file
            data=open(cur_file,'r')
            C=json.load(data)
            for i in C['data']["hoverDataList"]:
                name=i['name']
                count=i['metric'][0]['count']
                amount=i['metric'][0]['amount']
                columns3["Districts"].append(name)
                columns3["Transaction_Count"].append(count)
                columns3["Transaction_Amount"].append(amount)
                columns3["States"].append(state)
                columns3["Years"].append(year)
                columns3['Quarter'].append(int(file.strip('.json')))
map_trans=pd.DataFrame(columns3)
map_trans["States"] = map_trans["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
map_trans["States"] = map_trans["States"].str.replace("-"," ")
map_trans["States"] = map_trans["States"].str.title()
map_trans['States'] = map_trans['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")

        
map_trans
#map_user
path4="C:/Users/durga prasad/Desktop/project/pulse/data/map/user/hover/country/india/state/"
columns4 = {"States":[], "Years":[], "Quarter":[], "Districts":[], "RegisteredUser":[], "AppOpens":[]}
map_user_list=os.listdir(path4)
for state in map_user_list:
    cur_states = path4+state+"/"
    map_year_list = os.listdir(cur_states)
    
    for year in map_year_list:
        cur_years = cur_states+year+"/"
        map_file_list = os.listdir(cur_years)
        
        for file in map_file_list:
            cur_files = cur_years+file
            data = open(cur_files,"r")
            D = json.load(data)

            for i in D["data"]["hoverData"].items():
                district = i[0]
                registereduser = i[1]["registeredUsers"]
                appopens = i[1]["appOpens"]
                columns4["Districts"].append(district)
                columns4["RegisteredUser"].append(registereduser)
                columns4["AppOpens"].append(appopens)
                columns4["States"].append(state)
                columns4["Years"].append(year)
                columns4["Quarter"].append(int(file.strip(".json")))

map_users = pd.DataFrame(columns4)
map_users["States"] = map_users["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
map_users["States"] = map_users["States"].str.replace("-"," ")
map_users["States"] = map_users["States"].str.title()
map_users['States'] = map_users['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")

map_user
#top_trans
columns5 = {"States":[],"districts":[], "Years":[], "Quarter":[], "Pincodes":[], "Transaction_count":[], "Transaction_amount":[]}
path5="C:/Users/durga prasad/Desktop/project/pulse/data/top/transaction/country/india/state/"
top_trans_list=os.listdir(path4)
for state in top_trans_list:
    cur_state=path5+state+"/"
    top_year_list=os.listdir(cur_state)
    for year in top_year_list:
        cur_year=cur_state+year+"/"
        top_file_list=os.listdir(cur_year)
        for file in top_file_list:
            cur_file=cur_year+file
            data=open(cur_file,'r')
            E=json.load(data)
            for i in E["data"]["pincodes"]:
                entityName = i["entityName"]
                count = i["metric"]["count"]
                amount = i["metric"]["amount"]
                columns5["Pincodes"].append(entityName)
                columns5["Transaction_count"].append(count)
                columns5["Transaction_amount"].append(amount)
                columns5["States"].append(state)
                columns5["Years"].append(year)
                columns5["Quarter"].append(int(file.strip(".json")))
                columns5["districts"].append(district)
top_transaction = pd.DataFrame(columns5)

top_transaction["States"] = top_transaction["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
top_transaction["States"] = top_transaction["States"].str.replace("-"," ")
top_transaction["States"] = top_transaction["States"].str.title()
top_transaction['States'] = top_transaction['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")


#top_users

path6 = "C:/Users/durga prasad/Desktop/project/pulse/data/top/user/country/india/state/"
top_user_list = os.listdir(path6)

columns6 = {"States":[], "Years":[], "Quarter":[], "Pincodes":[], "RegisteredUser":[]}

for state in top_user_list:
    cur_states = path6+state+"/"
    top_year_list = os.listdir(cur_states)

    for year in top_year_list:
        cur_years = cur_states+year+"/"
        top_file_list = os.listdir(cur_years)

        for file in top_file_list:
            cur_files = cur_years+file
            data = open(cur_files,"r")
            F = json.load(data)
            for i in F["data"]["pincodes"]:
                name = i["name"]
                registeredusers = i["registeredUsers"]
                columns6["Pincodes"].append(name)
                columns6["RegisteredUser"].append(registeredusers)
                columns6["States"].append(state)
                columns6["Years"].append(year)
                columns6["Quarter"].append(int(file.strip(".json")))
                columns6["Districts"].append(district)
top_user = pd.DataFrame(columns6)

top_user["States"] = top_user["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
top_user["States"] = top_user["States"].str.replace("-"," ")
top_user["States"] = top_user["States"].str.title()
top_user['States'] = top_user['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman")

#Table creation
mydb = mysql.connector.connect(host = "localhost",
                        user = "root",
                        password = "Yerram@123",
                        database = "phonepe_data",
                        port = "3306"
                        )
cursor = mydb.cursor()

#aggregated transaction table
create_query1 = '''CREATE TABLE if not exists aggregated_transaction (States varchar(50),
                                                                      Years int,
                                                                      Quarter int,
                                                                      Transaction_type varchar(50),
                                                                      Transaction_count bigint,
                                                                      Transaction_amount bigint
                                                                      )'''
cursor.execute(create_query1)
mydb.commit()

for index,row in agg_transaction.iterrows():
    insert_query1 = '''INSERT INTO aggregated_transaction (States, Years, Quarter, Transaction_type, Transaction_count, Transaction_amount)
                                                        values(%s,%s,%s,%s,%s,%s)'''
    values = (row["States"],
              row["Years"],
              row["Quarter"],
              row["Transaction_Type"],
              row["Transaction_Count"],
              row["Transaction_Amount"]
              )
    cursor.execute(insert_query1,values)
    mydb.commit()

create_query2 = '''CREATE TABLE if not exists aggregated_user (States varchar(50),
                                                                Years int,
                                                                Quarter int,
                                                                Brands varchar(50),
                                                                Transaction_Count bigint,
                                                                Percentage float)'''
cursor.execute(create_query2)
mydb.commit()

for index,row in agg_user.iterrows():
    insert_query2 = '''INSERT INTO aggregated_user (States,Years,Quarter,Brands,Transaction_Count,Percentage)
                                                    values(%s,%s,%s,%s,%s,%s)'''
    values = (row["States"],
              row["Years"],
              row["Quarter"],
              row["Brand"],
              row["Transaction_Count"],
              row["Percentage"])
    cursor.execute(insert_query2,values)
    mydb.commit()

#map_transaction_table
create_query3 = '''CREATE TABLE if not exists map_transaction (States varchar(50),
                                                                Years int,
                                                                Quarter int,
                                                                District varchar(50),
                                                                Transaction_count bigint,
                                                                Transaction_amount float)'''
cursor.execute(create_query3)
mydb.commit()

for index,row in map_trans.iterrows():
            insert_query3 = '''
                INSERT INTO map_Transaction (States, Years, Quarter, District, Transaction_count, Transaction_amount)
                VALUES (%s, %s, %s, %s, %s, %s)

            '''
            values = (
                row['States'],
                row['Years'],
                row['Quarter'],
                row['Districts'],
                row['Transaction_Count'],
                row['Transaction_Amount']
            )
            cursor.execute(insert_query3,values)
            mydb.commit() 





#map_user_table
create_query4 = '''CREATE TABLE if not exists map_user (States varchar(50),
                                                        Years int,
                                                        Quarter int,
                                                        Districts varchar(50),
                                                        RegisteredUser bigint,
                                                        AppOpens bigint)'''
cursor.execute(create_query4)
mydb.commit()

for index,row in map_user.iterrows():
    insert_query4 = '''INSERT INTO map_user (States, Years, Quarter, Districts, RegisteredUser, AppOpens)
                        values(%s,%s,%s,%s,%s,%s)'''
    values = (row["States"],
              row["Years"],
              row["Quarter"],
              row["Districts"],
              row["RegisteredUser"],
              row["AppOpens"])
    cursor.execute(insert_query4,values)
    mydb.commit()


#top_transaction_table
create_query5 = '''CREATE TABLE if not exists top_transaction (States varchar(50),
                                                                Years int,
                                                                Quarter int,
                                                                pincodes int,
                                                                Transaction_count bigint,
                                                                Transaction_amount bigint)'''
cursor.execute(create_query5)
mydb.commit()

for index,row in top_transaction.iterrows():
    insert_query5 = '''INSERT INTO top_transaction (States, Years, Quarter, Pincodes, Transaction_count, Transaction_amount)
                                                    values(%s,%s,%s,%s,%s,%s)'''
    values = (row["States"],
              row["Years"],
              row["Quarter"],
              row["Pincodes"],
              row["Transaction_count"],
              row["Transaction_amount"])
    cursor.execute(insert_query5,values)
    mydb.commit()

#top_user_table
create_query6 = '''CREATE TABLE if not exists top_user (States varchar(50),
                                                        Years int,
                                                        Quarter int,
                                                        Pincodes int,
                                                        RegisteredUser bigint
                                                        )'''
cursor.execute(create_query6)
mydb.commit()

for index,row in top_user.iterrows():
    insert_query6 = '''INSERT INTO top_user (States, Years, Quarter, Pincodes, RegisteredUser)
                                            values(%s,%s,%s,%s,%s)'''
    values = (row["States"],
              row["Years"],
              row["Quarter"],
              row["Pincodes"],
              row["RegisteredUser"])
    cursor.execute(insert_query6,values)
    mydb.commit()





#mysql connnection
mydb = mysql.connector.connect(host = "localhost",
                        user = "root",
                        password = "Yerram@123",
                        database = "phonepe_data",
                        port = "3306"
                        )
cursor = mydb.cursor()
query='''select * from aggregated_transaction'''
cursor.execute(query)
table1 = cursor.fetchall()
mydb.commit()
Aggre_trans = pd.DataFrame(table1,columns = ("States", "Years", "Quarter", "Transaction_type", "Transaction_count", "Transaction_amount"))
cursor.close()
mydb.close()

cursor = mydb.cursor()
cursor.execute("select * from aggregated_user")
table2 = cursor.fetchall()
mydb.commit()
Aggre_user = pd.DataFrame(table2,columns = ("States", "Years", "Quarter", "Brands", "Transaction_count", "Percentage"))
cursor.execute("select * from map_transaction")
table3 = cursor.fetchall()
mydb.commit()
Map_trans = pd.DataFrame(table3,columns = ("States", "Years", "Quarter", "Districts", "Transaction_count", "Transaction_amount"))

cursor.execute("select * from map_user")
table4 = cursor.fetchall()
mydb.commit()
Map_user = pd.DataFrame(table4,columns = ("States", "Years", "Quarter", "Districts", "RegisteredUser", "AppOpens"))

cursor.execute("select * from top_transaction")
table5 = cursor.fetchall()
mydb.commit()
Top_trans = pd.DataFrame(table5,columns = ("States", "Years", "Quarter", "Pincodes", "Transaction_count", "Transaction_amount"))

cursor.execute("select * from top_user")
table6 = cursor.fetchall()
mydb.commit()
Top_user = pd.DataFrame(table6, columns = ("States", "Years", "Quarter", "Pincodes", "RegisteredUser"))
