import threading
from datetime import datetime
import pandas as pd
import mysql.connector
import schedule
import time
import requests
from flask import Flask, request, jsonify, session
import os
import json
import traceback
from io import BytesIO
import boto3
import pickle
from selenium.webdriver.common.by import By
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import ElementClickInterceptedException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
import random



app = Flask(__name__)

####################COPY FROM HERE TO EVERY MACHINE##################################
@app.route('/oldupdate', methods=['POST'])

def oldupdate():
    print('old profile update! :)')
    # GET Json
    parse_json = request.json
    linkedin_account = parse_json.get('link_account')
    #pass a list of 500 talent_link, public_link and name to the API
    public_link = parse_json.get('public_link')
    name = parse_json.get('name')
    driver_2 = driver_map[linkedin_account[0]]
    username = linkedin_account[1]
    password = linkedin_account[2]
    thread = threading.Thread(target= oldupdatedetail, args = (driver_2,username,password,public_link,name,linkedin_account[0]))
    thread.start()
    return jsonify({"status":200,"data":"success"})



def oldupdatedetail(driver_2,username,password,public_link,name,account):
    saved_data = {}
    talent_not_valid = {}
    quota_exceed = []
    print(account, "is working on it!!!")
    for i in range(len(public_link)):
        result_data = old_sourcing_pipeline(driver_2,public_link[i],username,password,i)#############
        if result_data == 404:
            talent_not_valid[name[i]] = public_link[i]
        elif result_data == 300:
            quota_exceed.append(public_link[i])
        elif result_data.get('lastName', None) == None or result_data.get('lastName', None)=='' or result_data.get('lastName',None) == {}:
            quota_exceed.append(public_link[i])
        else:
            raw_data = old_process_data(result_data)
            saved_data[result_data.get('publicProfileUrl',None)] = raw_data
        ##### random more about the sleep time
        if i%2 == 0:
            time.sleep(random.randrange(15,25))
        else:
            time.sleep(random.randrange(10,15))
        if i%50 == 0 and i != 0:
            #save as pickle
            bucket_name = 'ociinsights-dataseed'
            key = "buffer-oldfiles-update/raw-" + account + \
                "-" + "update" + "-" + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ".pkl"
            nmatched_upload_dict_to_s3_as_pickle(saved_data, bucket_name, key)
            print("today's process is on:",i,"th file")
            key_2 = "buffer-oldfiles-update/invalid-" + account + \
                "-" + "update" + "-" + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ".pkl"
            nmatched_upload_dict_to_s3_as_pickle(talent_not_valid, bucket_name, key_2)
            conn = get_nmatched_connection()
            cursor = conn.cursor()
            for j in range(len(talent_not_valid)):
                plink = list(talent_not_valid.values())[j]
                cursor.execute("UPDATE not_matched_data SET Status = 'not valid' WHERE linkedin = %s", (plink,))
                conn.commit()
            cursor.close()
            conn.close()
            talent_not_valid = {}
            saved_data = {}
            ####for test use
            #break
        ##### delete later
        if i == len(public_link)-1 and i%50 !=0:
                #save as pickle
            bucket_name = 'ociinsights-dataseed'
            key = "buffer-oldfiles-update/raw-" + account + \
                "-" + "update" + "-" + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ".pkl"
            nmatched_upload_dict_to_s3_as_pickle(saved_data, bucket_name, key)
            key_2 = "buffer-oldfiles-update/invalid-" + account + \
                "-" + "update" + "-" + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ".pkl"
            nmatched_upload_dict_to_s3_as_pickle(talent_not_valid, bucket_name, key_2)
            conn = get_nmatched_connection()
            cursor = conn.cursor()
            for j in range(len(talent_not_valid)):
                plink = list(talent_not_valid.values())[j]
                cursor.execute("UPDATE not_matched_data SET Status = 'not valid' WHERE linkedin = %s", (plink,))
                conn.commit()
            cursor.close()
            conn.close()
            talent_not_valid = {}
            saved_data = {}
        
    #update to the database the rows which api not work update the log for this part as none
    conn = get_nmatched_connection()
    cursor = conn.cursor()
    # for i in range(len(talent_not_valid)):
    #     public_link = list(talent_not_valid.values())[i]
    #     cursor.execute("UPDATE not_matched_data SET Status = 'not valid' WHERE linkedin = %s", (public_link,))
    #     conn.commit()
    for j in range(len(quota_exceed)):
        plink = quota_exceed[j]
        cursor.execute("UPDATE not_matched_data SET Status = NULL WHERE linkedin = %s", (plink,))
        conn.commit()
    cursor.close()
    conn.close()
    print("finished!!! Credit to: " + account)
    url = "https://open.larksuite.com/anycross/trigger/callback/MDYwOThjYTBlNDRlMDhhZGY1NjhkNWE0NmQ2OWIwYTI0"
    body = {
        "msg": "finished!!! Credit to: " + account}
    driver_2.switch_to.window(driver_2.window_handles[0])
    time.sleep(1)
    p = driver_2.current_window_handle
    chwd = driver_2.window_handles
    for w in chwd:
        if(w!=p):
            driver_2.switch_to.window(w)
            time.sleep(1)
            driver_2.close()
    response = requests.post(url, json=body)



def get_nmatched_connection():
    global connection
    connection = mysql.connector.connect(
        host = "oc-test-db-1.ckjyl94iquzb.us-west-1.rds.amazonaws.com",
        user="admin",
        password="hM6jw]USA2023CAN>^Ys",
        database = "ocinsights_dbtest",
        port = "3306")
    return connection
    
def nmatched_upload_dict_to_s3_as_pickle(data, bucket_name, key):
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

#sourcing_pipeline(driver_2,public_link[i],username,password,i)

def old_sourcing_pipeline(driver, public_link,username,password,num):
    driver.switch_to.window(driver.window_handles[0])
    try:
        driver.get(public_link)
    except Exception as e:
        print("public link is not valid")
        if len(driver.window_handles) > 1:
            driver.switch_to.window(driver.window_handles[-1])
            driver.close()
            driver.switch_to.window(driver.window_handles[0])
        return 404
    #public link is not valid
    time.sleep(random.randrange(2,4))
    try:
        gotorecruiter = WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.XPATH, "//button[contains(@aria-label,'in Recruiter')]")))
        driver.find_elements(By.XPATH,"//button[contains(@aria-label,'in Recruiter')]")[1].click()
        time.sleep(1)
    except:
        print("public link is not valid")
        if len(driver.window_handles) > 1:
            driver.switch_to.window(driver.window_handles[-1])
            driver.close()
            driver.switch_to.window(driver.window_handles[0])
        return 404
    time.sleep(random.randrange(1,3))
    #switch to the new tab
    driver.switch_to.window(driver.window_handles[-1])
    try:
        try:
            use = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, "//div[contains(@class,'login__form_action_container')]//button")))
            driver.find_element(by=By.ID, value='username').clear()
            time.sleep(1)
            driver.find_element(by=By.ID, value='username').send_keys(username)
            driver.find_element(by=By.ID, value='password').send_keys(password)
            driver.find_element(By.XPATH,"//div[contains(@class,'login__form_action_container')]//button").click()
        except:
            pass
        #click the second button to go to the recruiter lite page
        #check it !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        try:
            use = WebDriverWait(driver, 3).until(EC.presence_of_element_located((By.XPATH, "//div[contains(@class,'contract-list__item-summary')]")))
            #detect the button to go to the recruiter lite page
            q = 0
            for q in range(len(driver.find_elements(by=By.XPATH, value="//div[contains(@class,'contract-list__item-summary')]"))):
                if driver.find_elements(by=By.XPATH, value="//div[contains(@class,'contract-list__item-summary')]")[q].text.startswith('Recruiter Lite'):
                    break
            driver.find_elements(by=By.XPATH, value="//div[@class='contract-list__item-buttons']//button")[q].click()
        except:
            pass
    except:
        pass
    # try:
    #     driver.get(talent_link)
    # except:
    #     pass
    print("get into the recruiter lite page")
    time.sleep(1)
    talent_link = driver.current_url
    #print(talent_link)
    if not talent_link.startswith('https://www.linkedin.com/talent/profile'):
        print("talent link is not valid")
        if len(driver.window_handles) > 1:
            driver.switch_to.window(driver.window_handles[-1])
            driver.close()
            driver.switch_to.window(driver.window_handles[0])
        return 300
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
        print('API Method scrapping pipeline is success')
        data = json.loads(response.content)
        data['recruiter_url'] = talent_link
        time.sleep(random.randrange(1,3))
        if len(driver.window_handles) > 1:
            driver.close()
    #go to the original tab
        time.sleep(random.randrange(2,4))
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
    # if len(driver.window_handles) > 1:
    #     driver.close()
    time.sleep(1)
    #go to the original tab
    driver.switch_to.window(driver.window_handles[0])
    return 404
    

def old_process_data(data):
    dict = {}
    dict['lastName'] = data.get('lastName', None)
    dict['firstName'] = data.get('firstName', None)
    dict['contactInfo'] = data.get('contactInfo', None)
    dict['recruiter_url'] = data.get('recruiter_url', None)
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



####################END OF COPY##############################################