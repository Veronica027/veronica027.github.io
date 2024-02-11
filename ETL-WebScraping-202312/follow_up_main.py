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
    api_name = 'followupscrape'
    account_machine = pd.read_csv('data/account_machine.csv')
    #test test test!!!!
    #account_list = ['shuang']
    account_list = account_machine['account name'].to_list()
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
            cursor.execute("SELECT * FROM new_6com_talents WHERE Status = 'wait' AND Account != %s LIMIT 200",(account,))
            data_result = cursor.fetchall()
            cursor.close()
            cursor = conn.cursor()
            cursor.execute("UPDATE new_6com_talents SET Status = 'completed', Account = %s, completion_time = %s WHERE Status = 'wait' AND Account != %s LIMIT 200",(account,datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),account,))
            conn.commit()
            cursor.close()
        else:
            #for test use !!!!!
            cursor.execute("SELECT * FROM new_6com_talents WHERE Status = 'wait' AND Account != %s LIMIT 300",(account,))
            data_result = cursor.fetchall()
            cursor.close()
            cursor = conn.cursor()
            cursor.execute("UPDATE new_6com_talents SET Status = 'completed', Account = %s, completion_time = %s WHERE Status = 'wait' AND Account != %s LIMIT 300",(account,datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),account,))
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