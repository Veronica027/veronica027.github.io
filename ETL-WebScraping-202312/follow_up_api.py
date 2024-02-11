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



app = Flask(__name__)

####################COPY FROM HERE TO EVERY MACHINE##################################
@app.route('/followupscrape', methods=['POST'])
def followupscrape():
    print('profile update! :)')
    # GET Json
    parse_json = request.json
    linkedin_account = parse_json.get('link_account')
    #pass a list of 500 talent_link, public_link and name to the API
    talent_link = parse_json.get('talent_link')
    name = parse_json.get('name')
    driver_2 = driver_map[linkedin_account[0]]
    username = linkedin_account[1]
    password = linkedin_account[2]
    thread = threading.Thread(target= scrapedetail, args = (driver_2,talent_link,username,password,name,linkedin_account[0]))
    thread.start()
    return jsonify({"status":200,"data":"success"})



def scrapedetail(driver_2,talent_link,username,password,name,account):
    saved_data = {}
    api_not_work = []
    quota_exceed = []
    print(account, "is working on it!!!")
    for i in range(len(talent_link)):
        result_data = scraping_pipeline(driver_2,talent_link[i],username,password,i,"https://www.linkedin.com/talent/profile/ACoAACeOyc0BKG-njJACAXhvTR2-oWhu0MzyAjU")
        if result_data == 404:
            api_not_work.append(talent_link[i])
        elif result_data.get('lastName', None) == None or result_data.get('lastName', None)=='' or result_data.get('lastName',None) == {}:
            quota_exceed.append(talent_link[i])
        else:
            raw_data = data_processing(result_data)
            saved_data[result_data.get('publicProfileUrl',None)] = raw_data
        if i%100 == 0 and i != 0:
            #save as pickle
            bucket_name = 'ociinsights-dataseed'
            key = "buffer_new_profile/raw-" + account + \
                "-" + "update" + "-" + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ".pkl"
            upload_to_s3(saved_data, bucket_name, key)
            print("today's process is on:",i,"th file")
            saved_data = {}
            ####for test use
            #break
        ##### delete later
        if i == len(talent_link)-1 and i%100 !=0:
                #save as pickle
            bucket_name = 'ociinsights-dataseed'
            key = "buffer_new_profile/raw-" + account + \
                "-" + "update" + "-" + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ".pkl"
            upload_to_s3(saved_data, bucket_name, key)
            saved_data = {}
        time.sleep(random.randrange(3,13))
    #update to the database the rows which api not work update the log for this part as none
    conn = get_scrapedb_connection()
    cursor = conn.cursor()
    for j in range(len(api_not_work)):
        public_link = api_not_work[j]
        cursor.execute("UPDATE new_6com_talents SET Status = 'API INVALID' WHERE talent_url = %s", (public_link,))
        conn.commit()
    for j in range(len(quota_exceed)):
        public_link = quota_exceed[j]
        cursor.execute("UPDATE new_6com_talents SET Status = 'wait' WHERE talent_url = %s", (public_link,))
        conn.commit()
    cursor.close()
    conn.close()
    print("finished!!! Credit to: " + account)
    url = "https://open.larksuite.com/anycross/trigger/callback/MDYwOThjYTBlNDRlMDhhZGY1NjhkNWE0NmQ2OWIwYTI0"
    body = {
        "msg": "finished!!! Credit to: " + account}
    response = requests.post(url, json=body)



def get_scrapedb_connection():
    global connection
    connection = mysql.connector.connect(
        host = "oc-test-db-1.ckjyl94iquzb.us-west-1.rds.amazonaws.com",
        user="admin",
        password="hM6jw]USA2023CAN>^Ys",
        database = "ocinsights_dbtest",
        port = "3306")
    return connection
    
def upload_to_s3(data, bucket_name, key):
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

def scraping_pipeline(driver, talent_link,username,password,num,alternative_talent_link):
    if num == 0:
        go_in_talent_link = talent_link
        try:
            driver.get(go_in_talent_link)
        except:
            driver.get(alternative_talent_link)
        try:
            try:
                gotorecruiter = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//div[contains(@class,'login__form_action_container')]//button")))
                driver.find_element(by=By.ID, value='username').clear()
                driver.find_element(by=By.ID, value='username').send_keys(username)
                driver.find_element(by=By.ID, value='password').send_keys(password)
                driver.find_element(By.XPATH,"//div[contains(@class,'login__form_action_container')]//button").click()
            except:
                pass
            try:
                q = 0
                for q in range(len(driver.find_elements(by=By.XPATH, value="//div[contains(@class,'contract-list__item-summary-name')]"))):
                    if "Recruiter" in driver.find_elements(by=By.XPATH, value="//div[contains(@class,'contract-list__item-summary-name')]")[q].text:
                        break
                driver.find_elements(by=By.XPATH, value="//div[@class='contract-list__item-buttons']//button")[q].click()
            except:
                pass
        except:
            pass

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
        print('API Method scrappint pipeline is success: ' + str(num))
        data = json.loads(response.content)
        return data
            # merge the result data
            # all candidate
            # bd candidates

    except Exception as e:
        print(f"fetchTalentDetailPipeline Error occurred: {e}")
        traceback.print_exc()
        print("API have error")
        return 404
    

def data_processing(data):
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



####################END OF COPY##################################