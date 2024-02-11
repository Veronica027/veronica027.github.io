#work flow:
# 1.at 4:30 pm for loop every account and make it start the task to scrap all the data
# 2. at 5:00  pm end the task for every account
# 3. everyone 500 data
#everyone can only see data within 3 degree try to refresh the page to see if it can get the data
#if cannot get the data, then the log write: ella
#In[]
import pandas as pd
import mysql.connector
import schedule
import time
import requests
import datetime


def get_connection():
    global connection
    connection = mysql.connector.connect(
        host = "oc-test-db-1.ckjyl94iquzb.us-west-1.rds.amazonaws.com",
        user="admin",
        password="hM6jw]USA2023CAN>^Ys",
        database = "ocinsights_dbtest",
        port = "3306")
    return connection

#In[]
#remove the hyperlink in csv


# df = pd.read_csv('data/account_machine.csv')
# print("read data")
# conn = get_connection()
# cursor = conn.cursor()
# for i in range(len(df['endpoint'])):
#     cursor.execute("UPDATE account_machine SET username = %s WHERE account_name = %s", (df['Account'][i],df['account name'][i]))
#     print(i)
# conn.commit()
# cursor.close()

# public_link = "https://www.linkedin.com/in/shivshanks"
# cursor.execute("UPDATE other_account_data SET Status = NULL WHERE linkedin = %s", (public_link,))
# conn.commit()
# cursor.close()
# cursor.execute("SELECT * FROM other_account_data WHERE linkedin = %s", (public_link,))
# result = cursor.fetchall()
# print(result)
# conn.close()
# import datetime
# datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
# #In[]
# #conn = get_connection()
# cursor = conn.cursor()
# cursor.execute("UPDATE other_account_data SET Status = 'completed', Account = 'test', completion_time=%s WHERE Status IS NULL LIMIT 5",(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),))
# conn.commit()
# cursor.close()

def schedule_task():
    api_name = 'profileupdate'
    #test test test!!!!
    account_list = ['shuang']
    for account in account_list:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT endpoint, username, password FROM account_machine WHERE account_name = %s", (account,))
        result = cursor.fetchall()
        cursor.close()
        conn.close()
        endpoint = result[0][0]
        username = result[0][1]
        password = result[0][2]
        url = 'http://'+endpoint+':8000/'+api_name
        linkedin = [account,username,password]
        #fetch 500 data from the other_account_data table
        for q in range(8):
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM matched_data WHERE Status IS NULL LIMIT 130")
            data_result = cursor.fetchall()
            cursor.close()
            cursor = conn.cursor()
            cursor.execute("UPDATE matched_data SET Status = 'completed', Account = %s, completion_time = %s WHERE Status IS NULL LIMIT 130",(account,datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),))
            conn.commit()
            cursor.close()
            print("dataset updated")
            talent_link = []
            public_link = []
            name = []
            for data in data_result:
                talent_link_wop = data[3].split('?')[0]
                talent_link.append(talent_link_wop)
                public_link.append(data[2])
                name.append(data[1])
            #print(talent_link[0])
            body = {
                "link_account": linkedin,
                'talent_link': talent_link,
                'public_link': public_link,
                'name': name
            }
            response = requests.post(url, json=body)
            print("get response")
            #print(response)
            if response.json()['status'] == 200:
                print(account, " success")
                #update the status in the other_account_data table
            else:
                url = "https://open.larksuite.com/anycross/trigger/callback/MDYwOThjYTBlNDRlMDhhZGY1NjhkNWE0NmQ2OWIwYTI0"
                body = {
                    "msg": "this account has something bad happended: "+ str(account)}
                response = requests.post(url, json=body)

            conn.close()
            time.sleep((1+(q+1)//5)*40*60)
            if q%2 == 0:
                time.sleep(15*60)
            if q%3 == 0:
                time.sleep(20*60)

#In[]

if __name__ == '__main__':
    schedule.every().day.at("05:00").do(schedule_task)
    while True:
        schedule.run_pending()
        time.sleep(60)

    #for test use
    #schedule_task()