#Program to do BetMe's internship developer task. It establishes a mongoDB database connection and uses the odds-api to update data.
#The database structure

import json
import pymongo
import requests
import time

API_KEY
CONNECTION_STR = input("Enter your database connection string:") #e.g. mongodb://localhost:27017/
DATABASE_NAME = "bet.me.developer.task.database"
DATABASE
REGIONS = ['uk'] #extensibility: ['au','uk','eu','us']
DELAY = 3600 #1 hour delay

def establish_db_connection():
    try:
        global DATABASE
        DATABASE = pymongo.MongoClient(CONNECTION_STR)[DATABASE_NAME]
        return True
    except pymongo.errors.ConnectionFailure:
        return False

def get_all_sports():
    sports_response = requests.get('https://api.the-odds-api.com/v3/sports', params={
        'api_key': API_KEY
    })
    return json.loads(sports_response.text)

def get_all_fixtures():
    all_fixtures_list = list()
    for key in DATABASE["sports"].distinct('key'):
        for region in REGIONS:
            fixtures_response_loaded = json.loads(requests.get('https://api.the-odds-api.com/v3/sports', params={
                'api_key': API_KEY,
                'sport' : key,
                'region': region
            }))
            if fixtures_response_loaded['success']:
                all_fixtures_list.append(fixtures_response_loaded['data'])
            else:
                print("FAILED",
                    "Failed request to get fixtures: ",
                    fixtures_response_loaded['msg'])
    return all_fixtures_list

def get_all_live_fixtures(): #performs 30 queries at the moment but the API trial limit is 500 queries per month so be careful testing
    live_fixtures_list = list()
    for region in REGIONS:
        fixtures_response_loaded = json.loads(requests.get('https://api.the-odds-api.com/v3/sports', params={
            'api_key': API_KEY,
            'sport' : 'upcoming',
            'region': region
        }))
        if fixtures_response_loaded['success']:
            live_fixtures_list.append(fixtures_response_loaded['data'])
        else:
            print("FAILED",
                  "Failed request to get fixtures: ",
                  fixtures_response_loaded['msg'])
    return live_fixtures_list
    
def store_all_sports(sports_data):
    for item in sports_data:
        DATABASE["sports"].insert_one({
            "key" : item['key'],
            "group" : item['group'],
            "details" : item['details'],
            "title" : item['title']
        })
    return True

def store_all_fixtures(fixtures_all_json, fixtures_live_json): #only stores all not in play matches
    upcoming_fixtures = list(set(fixtures_all_json) - fixtures_live_json)
    for item in upcoming_fixtures:
        DATABASE["upcoming_fixtures"].insert_one({
            "id" : item['id'],
            "sport_key" : item['sport_key'],
            "sport_nice" : item['sport_nice'],
            "team_0" : item['teams']['0'],
            "team_1" : item['teams']['1'],
            "commence_time" : item['commence_time'],
            "home_team" : item['home_team'],
            "fst_h2h_0" : item['sites']['0']['odds']['h2h']['0'], #get first h2h odds for this exercise, could get an average or store all of the h2h odds instead
            "fst_h2h_1" : item['sites']['0']['odds']['h2h']['1']
        })
    return True

def delayed_update(delay):
    print("Performing initial update... ")
    while True:
        print("Getting all fixtures from query...",
               end="")
        fixtures_all_json = get_all_fixtures()
        if fixtures_all_json['success']:
            print("SUCCESS")
        else:
            exit #printing handled in function so the failing query can log their message
        
        print("Getting live fixtures from query...",
               end="")
        fixtures_live_json = get_all_live_fixtures()
        if fixtures_live_json['success']:
            print("SUCCESS")
        else:
            print("FAILED",
                "Failed request to get live fixtures: ",
                fixtures_live_json['msg'])
            exit
        
        print("Storing fixtures in database...",
               end="")
        if store_all_fixtures(fixtures_all_json, fixtures_live_json):
            print("SUCCESS")
        else:
            print("FAILED",
                "Failed to store fixtures in database.")
            exit

        print("Sleeping until scheduled update... ")
        time.sleep(delay)
        print("Performing scheduled update... ")


def main():
    print("Starting...",
          "Establishing database connection... ", 
           end="")
    if (establish_db_connection):
        print("SUCCESS")
    else:
        print("FAILED",
              "Failed to establish database connection.")
        exit

    global API_KEY 
    API_KEY = input("Enter your odds-api key: ")

    print("Getting sports from query... ",
           end="")
    sports_json = get_all_sports()
    if sports_json['success']:
        print("SUCCESS")
    else:
        print("FAILED",
              "Failed request to get sports: ",
              sports_json['msg'])
        exit

    print("Storing sports in database...",
           end="")
    if store_all_sports(sports_json['data']):
        print("SUCCESS")
    else:
        print("FAILED",
              "Failed to store sports in database.")
        exit

    print("Starting regular updates every " ++ DELAY ++ " seconds...")
    delayed_update(DELAY)

if __name__ == "__main__":
    main()