import time
import random
import requests
import datetime
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import ElementClickInterceptedException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
import pickle
import json
import traceback
import pandas as pd
import numpy as np
import random
import time
import schedule
from io import BytesIO
import boto3
import mysql.connector


def sign_in(driver):
    accounts = 'ella.zhao@ocbang.com'
    password = 'LinkOCB2022!'
    #driver.get('https://www.linkedin.com/home')
    time.sleep(1)
    with open('ellaupdate.pkl', 'rb') as f:
        cookies = pickle.load(f)
    print('found cookie, add to driver')
    # set cookie
    for cookie in cookies:
        cookie_dict = {
            'name': cookie['name'],
            'value': cookie['value'],
            'domain': cookie['domain'],
            'path': cookie['path']
                }
        if 'expiry' in cookie:
            cookie_dict['expiry'] = cookie['expiry']
        driver.add_cookie(cookie_dict)
    driver.refresh()
    try:
        lgin_form = driver.find_element(
        by=By.ID, value='session_key')
    except:
        lgin_form = None
    
    if lgin_form:
        driver.find_element(
            by=By.ID, value='session_key').send_keys(accounts)
        time.sleep(3)
        driver.find_element(
            by=By.ID, value='session_password').send_keys(password)
        time.sleep(3)
        driver.find_element(
            by=By.XPATH, value='//button[contains(@class, "sign-in-form__submit-btn")]').click()
        time.sleep(3)
    time.sleep(5)
    driver.refresh()


def get_connection():
    global connection
    connection = mysql.connector.connect(
        host = "oc-test-db-1.ckjyl94iquzb.us-west-1.rds.amazonaws.com",
        user="admin",
        password="hM6jw]USA2023CAN>^Ys",
        database = "ocinsights_dbtest",
        port = "3306")
    return connection

def old_sourcing_pipeline(driver, public_link):
    try:
        driver.get(public_link)
    except Exception as e:
        print("public link is not valid")
        return 404
    try:
        gotorecruiter = WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.XPATH, "//button[contains(@aria-label,'in Recruiter')]")))
    except:
        print("public link is not valid")
        return 404
    time.sleep(2)
    driver.find_elements(By.XPATH,"//button[contains(@aria-label,'in Recruiter')]")[1].click()
    time.sleep(1)
    #switch to the new tab
    driver.switch_to.window(driver.window_handles[1])
    try:
        password = 'LinkOCB2022!'
        try:
            use = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, "//div[contains(@class,'login__form_action_container')]//button")))
            driver.find_element(by=By.ID, value='username').clear()
            driver.find_element(by=By.ID, value='username').send_keys("ella.zhao@ocbang.com")
            driver.find_element(by=By.ID, value='password').send_keys(password)
            driver.find_element(By.XPATH,"//div[contains(@class,'login__form_action_container')]//button").click()
        except:
            pass
        #click the second button to go to the recruiter lite page
        #check it !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        try:
            print("click the second button to go to the recruiter lite page")
            driver.find_elements(by=By.XPATH, value="//div[@class='contract-list__item-buttons']//button")[0].click()
        except:
            pass
    except:
        pass
    # try:
    #     driver.get(talent_link)
    # except:
    #     pass
    print("get into the recruiter lite page")
    talent_link = driver.current_url
    time.sleep(3)
    # Use API Data Scrapping
    dict = {}
    #print(driver.get_cookies())
    for p in driver.get_cookies():
        dict[p['name']] = p['value']

    dict['JSESSIONID'].split('"')[1]

    result = ""
    for p in dict:
        string_name = str(p)
        string_equal = '='
        string_value = str(dict[p])
        string_end = ';'
        item = string_name+string_equal+string_value+string_end
        result += item

    # get variable for pipeline api
    token = dict['JSESSIONID']
    token = token[1:len(token)-1]
    cookie = result
    referer = talent_link
    talentId = talent_link.split('/')[-1]
    url = 'https://www.linkedin.com/talent/api/talentLinkedInMemberProfiles/urn%3Ali%3Ats_linkedin_member_profile%3A(' + talentId + '%2C1%2Curn%3Ali%3Ats_hiring_project%3A0)' #'https://prod.ocinsights.ai/api/ext/fetchTalentProfile'#
    payload = {
            "token": token,
            "referer": referer,
            "cookies": cookie,
            "talentId": talentId
        }
    
    headers={
                "accept": "application/json",
                "accept-language": "en-US,en;q=0.9",
                "content-type": "application/x-www-form-urlencoded",
                "csrf-token": token,
                "sec-ch-ua": "\"Not.A/Brand\";v=\"8\", \"Chromium\";v=\"114\", \"Google Chrome\";v=\"114\"",
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": "\"macOS\"",
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-origin",
                "x-http-method-override": "GET",
                "x-li-lang": "en_US",
                "x-li-page-instance": "urn:li:page:d_talent_profile_profile_tab;SK6Bu0UwT8655X7osCgV4A==",
                "x-li-pem-metadata": "Hiring Platform - Profile=standalone-global-profile-view",
                "x-li-track": "{\"clientVersion\":\"1.5.2486\",\"mpVersion\":\"1.5.2486\",\"osName\":\"web\",\"timezoneOffset\":-7,\"timezone\":\"America/Los_Angeles\",\"mpName\":\"talent-solutions-web\",\"displayDensity\":2,\"displayWidth\":3456,\"displayHeight\":2234}",
                "x-restli-protocol-version": "2.0.0",
                "cookie": cookie,
                "Referer": referer,
                "Referrer-Policy": "strict-origin-when-cross-origin"    
            }
    try: 
        response = requests.request("POST", url, data = payload,headers=headers)   #data=json.dumps(payload)
        print('API Method scrappint pipeline is success')
        data = json.loads(response.content)
        time.sleep(2)
        driver.close()
    #go to the original tab
        time.sleep(1)
        driver.switch_to.window(driver.window_handles[0])
        return data
            # merge the result data
            # all candidate
            # bd candidates

    except Exception as e:
        url = "https://open.larksuite.com/anycross/trigger/callback/MDYwOThjYTBlNDRlMDhhZGY1NjhkNWE0NmQ2OWIwYTI0"
        body = {
            "msg": "API have error:"+str(e)}
        response = requests.post(url, json=body)
        traceback.print_exc()
    time.sleep(2)
    driver.close()
    time.sleep(1)
    #go to the original tab
    driver.switch_to.window(driver.window_handles[0])
        
def process_data(data):
    dict = {}
    dict['lastName'] = data.get('lastName', None)
    dict['firstName'] = data.get('firstName', None)
    dict['contactInfo'] = data.get('contactInfo', None)
    dict['educations'] = data.get('educations', None)
    dict['skills'] = data.get('skills', None)
    dict['canSendInMail'] = data.get('canSendInMail', None)
    dict['viewerCompanyFollowing'] = data.get(
                        'viewerCompanyFollowing', None)
    dict['entityUrn'] = data.get('entityUrn', None)
    dict['headline'] = data.get('headline', None)
    dict['industryName'] = data.get('industryName', None)
    dict['publicProfileUrl'] = data.get('publicProfileUrl', None)
    dict['numConnections'] = data.get('numConnections', None)
    dict['vectorProfilePicture'] = data.get('vectorProfilePicture', None)
    dict['referenceUrn'] = data.get('referenceUrn', None)
    dict['privacySettings'] = data.get('privacySettings', None)
    dict['unlinked'] = data.get('unlinked', None)
    dict['highlights'] = data.get('highlights', None)
    dict['location'] = data.get('location', None)
    dict['memberPreferences'] = data.get(
        'memberPreferences', None)
    dict['networkDistance'] = data.get('networkDistance', None)
    dict['stack'] = "TBD"
    dict['language'] = [position_language['name'] for position_language in data.get('languages', [])]
    dict['networkDistance'] = data.get('networkDistance', None)
    # dict['talentDetail'] = objects.get(
    #     'talentDetail', {})
    dict['accomplishments'] = data.get('accomplishments',None)
    dict['educationDetails'] = data.get('educations',None)
    dict['locationDetails'] = data.get('location',None)
    dict['summary'] = data.get('summary',None)
    #dict['workExperience'] = detail.get('workExperience', None)
    workExperience_update=[]
    talent_exp_detail = data.get('groupedWorkExperience',[])
    for exps in talent_exp_detail:
        position_list = exps.get('positions',[])
        for exp in position_list:
            result = {}
            result['companyName'] = exp.get('companyName',None)
            result['title'] = exp.get('title',None)
            result['description'] = exp.get('description',None)
            result['startDateOn'] = exp.get('startDateOn',{})
            result['endDateOn'] = exp.get('endDateOn',{})
            result['location'] = exp.get('location',{})
            result['employmentStatus'] = exp.get('employmentStatus',None)
            result['associatedProfileSkillNames'] = exp.get('associatedProfileSkillNames',[])
            result['companyUrl'] = "https://www.linkedin.com"+exp.get('companyUrl','')
            workExperience_update.append(result)
    dict['workExperience'] = workExperience_update
    return dict

def upload_dict_to_s3_as_pickle(data, bucket_name, key):
    # Convert the dictionary to a pickle file in memory
    buffer = BytesIO()
    pickle.dump(data, buffer)
    buffer.seek(0)

    # Configure the S3 client
    s3_client = boto3.client(
        's3',
        aws_access_key_id='AKIAYIIEBOU5ULQLCG5L',
        aws_secret_access_key='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',
        region_name='us-west-1'
    )

    # Upload the pickle file to the S3 bucket
    s3_client.upload_fileobj(buffer, bucket_name, key)

def scrape_task():
    driver = webdriver.Chrome(service = Service(ChromeDriverManager().install()))
    driver.maximize_window()
    driver.get('https://www.linkedin.com/home')
    driver.implicitly_wait(6)
    try:
        sign_in(driver)
    
    except:
        print("sign in error")
        url = "https://open.larksuite.com/anycross/trigger/callback/MDYwOThjYTBlNDRlMDhhZGY1NjhkNWE0NmQ2OWIwYTI0"
        body = {
            "msg": "Sign In Error and stop"}
        response = requests.post(url, json=body)
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM matched_data WHERE Status IS NULL LIMIT 1500")
    data_result = cursor.fetchall()
    cursor.close()
    cursor = conn.cursor()
    cursor.execute("UPDATE matched_data SET Status = 'completed', Account = %s, completion_time = %s WHERE Status IS NULL LIMIT 1500",('ella',datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),))
    conn.commit()
    cursor.close()
    saved_data = {}
    talent_not_valid = {}
    print("Ella is working on it!")
    i = 0
    quota_exceed = []
    for data in data_result:
        talent_link = data[3]
        public_link = data[2]
        name = data[1]
        result_data = old_sourcing_pipeline(driver, public_link)
        if result_data == 404:
            talent_not_valid[name] = public_link
        elif result_data.get('lastName', None) == None or result_data.get('lastName', None)=='' or result_data.get('lastName', None) == {}:
            quota_exceed.append(public_link)
        else:
            #public_link = result_data.get('publicProfileUrl',None)
            raw_data = process_data(result_data)
            saved_data[result_data.get('publicProfileUrl',None)] = raw_data
        if i%25 == 0 and i != 0:
            time.sleep(random.randrange(2,10))
        if i%45 == 0 and i != 0:
            time.sleep(random.randrange(3,8))
        time.sleep(random.randrange(3,7))
            #file name raw-accountg-update-timestamp
        #if i%3 == 0: # for test use, change it to the following line
        if i%100==0 and i != 0:
            #save as pickle
            bucket_name = 'ociinsights-dataseed'
            key = "buffer-files-update/raw-" + "ella" + \
                "-" + "update" + "-" + str(datetime.datetime.now()) + ".pkl"
            upload_dict_to_s3_as_pickle(saved_data, bucket_name, key)
            print("today's process is on:",i,"th file")
            key_2 = "buffer-files-update/invalid-" + "ella" + \
                "-" + "update" + "-" + str(datetime.datetime.now()) + ".pkl"
            upload_dict_to_s3_as_pickle(talent_not_valid, bucket_name, key_2)
            talent_not_valid = {}
            saved_data = {}
            #####for test use
            #break
        ###### delete later
        if i == 1499 and i%100!=0:
            #save as pickle
            bucket_name = 'ociinsights-dataseed'
            key = "buffer-files-update/raw-" + "ella" + \
                "-" + "update" + "-" + str(datetime.datetime.now()) + ".pkl"
            upload_dict_to_s3_as_pickle(saved_data, bucket_name, key)
            print("today's process is on:",i,"th file")
            key_2 = "buffer-files-update/invalid-" + "ella" + \
                "-" + "update" + "-" + str(datetime.datetime.now()) + ".pkl"
            upload_dict_to_s3_as_pickle(talent_not_valid, bucket_name, key_2)
            talent_not_valid = {}
            saved_data = {}
        i += 1
    driver.quit()
    cursor = conn.cursor()
    for i in range(len(quota_exceed)):
        public_link = quota_exceed[i]
        cursor.execute("UPDATE matched_data SET Status = NULL WHERE linkedin = %s", (public_link,))
        conn.commit()
    cursor.close()
    conn.close()
    url = "https://open.larksuite.com/anycross/trigger/callback/MDYwOThjYTBlNDRlMDhhZGY1NjhkNWE0NmQ2OWIwYTI0"
    body = {
        "msg": "Ella has done her job today!!!"}
    response = requests.post(url, json=body)



# def scrape_task(scraping_file):
#     global count
#     driver = webdriver.Chrome(service = Service(ChromeDriverManager().install()))
#     driver.maximize_window()
#     driver.get('https://www.linkedin.com/home')
#     driver.implicitly_wait(6)
#     try:
#         sign_in(driver)
    
#     except:
#         print("sign in error")
#         print("stop in which part of the file:", count)
#         url = "https://open.larksuite.com/anycross/trigger/callback/MDYwOThjYTBlNDRlMDhhZGY1NjhkNWE0NmQ2OWIwYTI0"
#         body = {
#             "msg": "Sign In Error and stop in which part of the file:"+str(count)}
#         response = requests.post(url, json=body)
    
#     #for the following part, we need to first go into the public url and then go to the recruiter lite page
#     #the data is from not matched data
#     #scrape 1000 talents per day
#     print("processing which part of the file:",count)
#     today_file = scraping_file.iloc[count*1000:(count+1)*1000,:]
#     today_file.reset_index(inplace = True,drop=True)
#     saved_data = {}
#     talent_not_valid = {}
#     print("Ella is working on it!")
#     print("today we are scrapping:",count,"th talents")
#     for i in range(len(today_file)):
#         talent_link = today_file['recruiter_url'][i]
#         public_link = today_file['linkedin'][i]
#         name = today_file['name'][i]
#         result_data = old_sourcing_pipeline(driver, talent_link)
#         if result_data == None:
#             talent_not_valid[name] = public_link
#         else:
#             #public_link = result_data.get('publicProfileUrl',None)
#             raw_data = process_data(result_data)
#             saved_data[result_data.get('publicProfileUrl',None)] = raw_data
#         if i%25 == 0 and i != 0:
#             time.sleep(random.randrange(4,17))
#         if i%45 == 0 and i != 0:
#             time.sleep(random.randrange(3,15))
#         time.sleep(random.randrange(1,7))
#             #file name raw-accountg-update-timestamp
#         #if i%3 == 0: # for test use, change it to the following line
#         if i%100 == 0 and i != 0:
#             #save as pickle
#             bucket_name = 'ociinsights-dataseed'
#             key = "buffer-files-update/raw-" + "ella" + \
#                 "-" + "update" + "-" + str(datetime.datetime.now()) + ".pkl"
#             upload_dict_to_s3_as_pickle(saved_data, bucket_name, key)
#             print("today's process is on:",i,"th file")
#             key_2 = "buffer-files-update/invalid-" + "ella" + \
#                 "-" + "update" + "-" + str(datetime.datetime.now()) + ".pkl"
#             upload_dict_to_s3_as_pickle(talent_not_valid, bucket_name, key_2)
#             talent_not_valid = {}
#             saved_data = {}
#             #####for test use
#             #break
#         ###### delete later
#         if i == len(scraping_file)-1 and i%100 !=0:
#             #save as pickle
#             bucket_name = 'ociinsights-dataseed'
#             key = "buffer-files-update/raw-" + "ella" + \
#                 "-" + "update" + "-" + str(datetime.datetime.now()) + ".pkl"
#             upload_dict_to_s3_as_pickle(saved_data, bucket_name, key)
#             print("today's process is on:",i,"th file")
#             key_2 = "buffer-files-update/invalid-" + "ella" + \
#                 "-" + "update" + "-" + str(datetime.datetime.now()) + ".pkl"
#             upload_dict_to_s3_as_pickle(talent_not_valid, bucket_name, key_2)
#             talent_not_valid = {}
#             saved_data = {}
#     count += 1
#     driver.quit()
#     if count == 26:
#         print("Ella has done her job!")
#         schedule.cancel_job(scrape_task)
        
        

if __name__ == '__main__':
 #  schedule.every().day.at("04:00").do(scrape_task)
 #  while True:
 #      schedule.run_pending()
 #      time.sleep(5)
    scrape_task()
    # scraping_file = pd.read_csv('data/not_matched_data.csv')
    # #modify the schedule later
    # schedule.every().day.at("03:00").do(scrape_task,scraping_file)
    # while True:
    #     schedule.run_pending()
    #     time.sleep(60)