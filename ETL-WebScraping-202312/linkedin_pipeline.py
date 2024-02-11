#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun 12 14:29:32 2023

@author: chunpingyang
"""
from flask import Flask, jsonify, request, session
import json
import requests
import traceback


pipe = Flask(__name__)

@pipe.route('/fetchTalentDetailPipeline', methods=['POST'])
def fetchTalentDetailPipeline():
   
    try:
        
        if request.method == 'POST':
            
            parse_json = request.json
            print("parse_json: ", parse_json)
            talentId = parse_json['talentId']
            cookies = parse_json['cookies']
            referer = parse_json['referer']
            token = parse_json['token']
            print("talentId: ", talentId)
            print("cookies: ", cookies)
            print('referer: ', referer)
            print('token: ', token)
            
            url = 'https://www.linkedin.com/talent/api/talentLinkedInMemberProfiles/urn%3Ali%3Ats_linkedin_member_profile%3A(' + talentId + '%2C1%2Curn%3Ali%3Ats_hiring_project%3A0)'
               
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
                "cookie": cookies,
                "Referer": referer,
                "Referrer-Policy": "strict-origin-when-cross-origin"    
            }
            
            body="altkey=urn&decoration=%28entityUrn%2CreferenceUrn%2Canonymized%2CunobfuscatedFirstName%2CunobfuscatedLastName%2CmemberPreferences%28availableStartingAt%2Clocations%2CgeoLocations*~%28standardGeoStyleName%29%2CopenToNewOpportunities%2Ctitles%2CinterestedCandidateIntroductionStatement%2Cindustries*~%2CcompanySizeRange%2CemploymentTypes%2CproxyPhoneNumberAvailability%2Cbenefits%2Cschedules%2CsalaryLowerBounds%2Ccommute%2CjobSeekingUrgencyLevel%2CopenToWorkRemotely%2ClocalizedWorkplaceTypes%2CremoteGeoLocationUrns*~%28standardGeoStyleName%29%29%2CfirstName%2ClastName%2Cheadline%2Clocation%2CprofilePicture%2CvectorProfilePicture%2CnumConnections%2Csummary%2CnetworkDistance%2CprofileSkills*%28name%2CskillAssessmentBadge%2CprofileResume%2CendorsementCount%2CprofileSkillAssociationsGroupUrn~%28entityUrn%2Cassociations*%28description%2Ctype%2CorganizationUrn~%28name%2Curl%2Clogo%29%29%29%2ChasInsight%29%2CpublicProfileUrl%2CcontactInfo%2Cwebsites*%2CcanSendInMail%2Cunlinked%2CunLinkedMigrated%2Chighlights%28connections%28connections*~%28entityUrn%2CfirstName%2ClastName%2Cheadline%2CprofilePicture%2CvectorProfilePicture%2CpublicProfileUrl%2CfollowerCount%2CnetworkDistance%2CautomatedActionProfile%29%2CtotalCount%29%2Ccompanies%28companies*%28company~%28followerCount%2Cname%2Curl%2CvectorLogo%29%2CoverlapInfo%29%29%2Cschools%28schools*%28school~%28name%2Curl%2CvectorLogo%29%2CschoolOrganizationUrn~%28name%2Curl%2Clogo%29%2CoverlapInfo%29%29%29%2CfollowingEntities%28companies*~%28followerCount%2Cname%2Curl%2CvectorLogo%29%2Cinfluencers*~%28entityUrn%2CfirstName%2ClastName%2Cheadline%2CprofilePicture%2CvectorProfilePicture%2CpublicProfileUrl%2CfollowerCount%2CnetworkDistance%2CautomatedActionProfile%29%2Cschools*~%28name%2Curl%2CvectorLogo%29%2CschoolOrganizationsUrns*~%28name%2Curl%2Clogo%29%29%2Ceducations*%28school~%28name%2Curl%2CvectorLogo%29%2CorganizationUrn~%28name%2Curl%2Clogo%29%2CschoolName%2Cgrade%2Cdescription%2CdegreeName%2CfieldOfStudy%2CstartDateOn%2CendDateOn%29%2CgroupedWorkExperience*%28companyUrn~%28followerCount%2Cname%2Curl%2CvectorLogo%29%2Cpositions*%28title%2CstartDateOn%2CendDateOn%2Cdescription%2Clocation%2CemploymentStatus%2CorganizationUrn%2CcompanyName%2CassociatedProfileSkillNames%2CcompanyUrn~%28url%2CvectorLogo%29%29%2CstartDateOn%2CendDateOn%29%2CvolunteeringExperiences*%28company~%28followerCount%2Cname%2Curl%2CvectorLogo%29%2CcompanyName%2Crole%2CstartDateOn%2CendDateOn%2Cdescription%29%2Crecommendations*%28recommender~%28entityUrn%2CfirstName%2ClastName%2Cheadline%2CprofilePicture%2CvectorProfilePicture%2CpublicProfileUrl%2CfollowerCount%2CnetworkDistance%2CautomatedActionProfile%29%2CrecommendationText%2Crelationship%2Ccreated%29%2Caccomplishments%28projects*%28title%2Cdescription%2Curl%2CstartDateOn%2CendDateOn%2CsingleDate%2Ccontributors*%28name%2ClinkedInMember~%28entityUrn%2Canonymized%2CunobfuscatedFirstName%2CunobfuscatedLastName%2CfirstName%2ClastName%2Cheadline%2CprofilePicture%2CvectorProfilePicture%2CpublicProfileUrl%2CfollowerCount%2CnetworkDistance%2CautomatedActionProfile%29%29%29%2Ccourses*%2Clanguages*%2Cpublications*%28name%2Cpublisher%2Cdescription%2Curl%2CdateOn%2Cauthors*%28name%2ClinkedInMember~%28entityUrn%2Canonymized%2CunobfuscatedFirstName%2CunobfuscatedLastName%2CfirstName%2ClastName%2Cheadline%2CprofilePicture%2CvectorProfilePicture%2CpublicProfileUrl%2CfollowerCount%2CnetworkDistance%2CautomatedActionProfile%29%29%29%2Cpatents*%28number%2CapplicationNumber%2Ctitle%2Cissuer%2Cpending%2Curl%2CfilingDateOn%2CissueDateOn%2Cdescription%2Cinventors*%28name%2ClinkedInMember~%28entityUrn%2Canonymized%2CunobfuscatedFirstName%2CunobfuscatedLastName%2CfirstName%2ClastName%2Cheadline%2CprofilePicture%2CvectorProfilePicture%2CpublicProfileUrl%2CfollowerCount%2CnetworkDistance%2CautomatedActionProfile%29%29%29%2CtestScores*%2Chonors*%2Ccertifications*%28name%2ClicenseNumber%2Cauthority%2Ccompany~%28followerCount%2Cname%2Curl%2CvectorLogo%29%2Curl%2CstartDateOn%2CendDateOn%29%29%2CprivacySettings%28allowConnectionsBrowse%2CshowPremiumSubscriberIcon%29%2ClegacyCapAuthToken%2CfullProfileNotVisible%2CcurrentPositions*%28company~%28followerCount%2Cname%2Curl%2CvectorLogo%29%2CcompanyName%2Ctitle%2CstartDateOn%2CendDateOn%2Cdescription%2Clocation%29%2CindustryName%29"
            
            response = requests.request("POST", url, data=body, headers=headers)    
            data = json.loads(response.content)

            
            return jsonify({"status": 200, "message":"fetchTalentDetailPipeline successfully", 'data': data}), 200
    except Exception as e:
        print(f"fetchTalentDetailPipeline Error occurred: {e}")
        traceback.print_exc()
        return jsonify({"status": 400, "message":"failed to fetchTalentDetailPipeline"}), 400


if __name__ == '__main__':

    pipe.run(host="0.0.0.0", port=8083, debug=False)