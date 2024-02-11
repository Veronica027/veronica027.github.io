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


def schedule_task():
    api_name = 'oldupdate'
    #test test test!!!!
    account_list = ['ella']
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
        for q in range(6):
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM not_matched_data WHERE Status IS NULL LIMIT 100")
            data_result = cursor.fetchall()
            cursor.close()
            cursor = conn.cursor()
            cursor.execute("UPDATE not_matched_data SET Status = 'completed', Account = %s, completion_time = %s WHERE Status IS NULL LIMIT 100",(account,datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),))
            conn.commit()
            cursor.close()
            print("dataset updated")
            public_link = []
            name = []
            for data in data_result:
                public_link.append(data[2])
                name.append(data[1])
            #print(talent_link[0])
            body = {
                "link_account": linkedin,
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
            if q == 0:
                time.sleep(60*110)
            elif q == 1:
                time.sleep(60*120)
            elif q == 2:
                time.sleep(60*130)
            elif q == 3:
                time.sleep(60*140)
            elif q == 4:
                time.sleep(60*120)

#In[]

if __name__ == '__main__':
    schedule.every().day.at("04:00").do(schedule_task)
    while True:
        schedule.run_pending()
        time.sleep(60)

    #for test use
   # schedule_task()