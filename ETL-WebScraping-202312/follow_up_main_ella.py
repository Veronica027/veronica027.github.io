import pandas as pd
import mysql.connector
import schedule
import time
import requests
import datetime


#follow up those who is beyond 3rd degree connection

def get_connection():
    global connection
    connection = mysql.connector.connect(
        host = "oc-test-db-1.ckjyl94iquzb.us-west-1.rds.amazonaws.com",
        user="admin",
        password="hM6jw]USA2023CAN>^Ys",
        database = "ocinsights_dbtest",
        port = "3306")
    return connection


def schedule_task():
    api_name = 'followupscrape'
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
        for q in range(2):
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM new_6com_talents WHERE Status = 'wait' LIMIT 130")
            data_result = cursor.fetchall()
            cursor.close()
            cursor = conn.cursor()
            cursor.execute("UPDATE new_6com_talents SET Status = 'completed', Account = %s, completion_time = %s WHERE Status = 'wait' LIMIT 130",(account,datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),))
            conn.commit()
            cursor.close()
            print("dataset updated")
            talent_link = []
            name = []
            for data in data_result:
                talent_link_wop = data[2]
                talent_link.append(talent_link_wop)
                name.append(data[1])
            #print(talent_link[0])
            body = {
                "link_account": linkedin,
                'talent_link': talent_link,
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
            time.sleep(60*60)

#In[]

if __name__ == '__main__':
    schedule.every().day.at("10:00").do(schedule_task)
    while True:
        schedule.run_pending()
        time.sleep(60)

    #for test use
  #  schedule_task()