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


def schedule_task(company_name,filter_url):
    api_name = 'ellatalent'
    #test test test!!!!
    account_list = ['audrey']
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
            #print(talent_link[0])
        body = {
            "link_account": linkedin,
            'company_name': company_name,
            'filter_url': filter_url
        }
        response = requests.post(url, json=body)
        print("get response")
        #print(response)
        if response.json()['status'] == 200:
            print(account, " success")
            return 200
            #update the status in the other_account_data table
        else:
            url = "https://open.larksuite.com/anycross/trigger/callback/MDYwOThjYTBlNDRlMDhhZGY1NjhkNWE0NmQ2OWIwYTI0"
            body = {
                "msg": "this account has something bad happended: "+ str(account)}
            response = requests.post(url, json=body)

def task():
    #design it as a database after test
    for i in range(2):
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT company, filter_url FROM company_filter WHERE Account = 'audrey' AND Status IS NULL LIMIT 1")
        result = cursor.fetchall()
        cursor.close()
        conn.close()
        company_name = result[0][0]
        filter_url = result[0][1]
        final = schedule_task(company_name,filter_url)
        if final == 200:
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute("UPDATE company_filter SET Status = 'done', compeletion_time = %s WHERE Account = 'audrey' AND Status IS NULL LIMIT 1", (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),))
            conn.commit()
            cursor.close()
            conn.close()
        time.sleep(60*60*4)


#In[]

if __name__ == '__main__':
    
    schedule.every().day.at("08:30").do(task)
    while True:
        schedule.run_pending()
        time.sleep(60)

    # schedule.every().day.at("05:00").do(schedule_task)
    # while True:
    #     schedule.run_pending()
    #     time.sleep(60)

    #for test use
    #schedule_task()