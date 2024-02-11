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


def schedule_task():
    account_name = {'audrey':'audrey',
                    'nina':'nina',
                    'kirby':'kirby',
                    'jenny':'jenny',
                    'yufei':'yufei',
                    'nick':'nick',
                    'yuxuan':'yuxuan',
                    'yiming':'yiming',
                    'daisy':'daisy',
                    'angelina':'angelina',
                    'emily':'emily',
                    'levana':'levana',
                    'nicole':'nicole',
                    'donald': 'donald',
                    'jack': 'jack',
                    'james': 'james',
                    'leon': 'leon'}
    api_name = 'oldupdate'
    account_machine = pd.read_csv('data/account_machine.csv')
    #test test test!!!!
    #account_list = ['shuang']
    account_list = account_machine['account name'].to_list()
    #remove yuxuan from this list, yuxuan is temparily not available
    
    #split the work for each account
    conn = get_connection()
    for account in account_list:
        cursor = conn.cursor()
        cursor.execute("SELECT endpoint, username, password FROM account_machine WHERE account_name = %s", (account,))
        result = cursor.fetchall()
        cursor.close()
        endpoint = result[0][0]
        username = result[0][1]
        password = result[0][2]
        url = 'http://'+endpoint+':8000/'+api_name
        linkedin = [account,username,password]
        print(linkedin)
        print(url)
        #fetch 500 data from the other_account_data table
        cursor = conn.cursor()
        if account != 'shuang' and account != 'kirby':
            cursor.execute("SELECT * FROM not_matched_data WHERE Status IS NULL LIMIT 30")
            data_result = cursor.fetchall()
            cursor.close()
            cursor = conn.cursor()
            cursor.execute("UPDATE not_matched_data SET Status = 'completed', Account = %s, completion_time = %s WHERE Status IS NULL LIMIT 30",(account,datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),))
            conn.commit()
            cursor.close()
        else:
            #for test use !!!!!
            cursor.execute("SELECT * FROM not_matched_data WHERE Status IS NULL LIMIT 60")
            data_result = cursor.fetchall()
            cursor.close()
            cursor = conn.cursor()
            cursor.execute("UPDATE not_matched_data SET Status = 'completed', Account = %s, completion_time = %s WHERE Status IS NULL LIMIT 60",(account,datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),))
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
            if account != 'shuang':
                url = 'http://54.153.60.227:8001/bot'
                body = {
                    "target_user_name": account_name[account],
                    "user_name": 'xiaotong',
                    "action": 'data_everydayupdate_alert'
                }
                response = requests.post(url, json=body)
            print(account, " success")
            #update the status in the other_account_data table
        else:
            url = "https://open.larksuite.com/anycross/trigger/callback/MDYwOThjYTBlNDRlMDhhZGY1NjhkNWE0NmQ2OWIwYTI0"
            body = {
                "msg": "this account has something bad happended: "+ str(account)}
            response = requests.post(url, json=body)

    conn.close()


#In[]

if __name__ == '__main__':
    schedule.every().day.at("23:30").do(schedule_task)
    while True:
        schedule.run_pending()
        time.sleep(60)

    #for test use
    #schedule_task()