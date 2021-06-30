import pymongo
CONNECTION_STR = input("Enter your database connection string:") #e.g. mongodb://localhost:27017/
DATABASE_NAME = "bet.me.developer.task.database"

def establish_db_connection():
    try:
        client = pymongo.MongoClient(CONNECTION_STR)[DATABASE_NAME]
        return True
    except pymongo.errors.ConnectionFailure:
        return False

def store_all_sports():
    return True

def store_all_fixtures():
    return True


def main():
    print("Starting...")

    print("Establishing database connection...")
    if (establish_db_connection):
        print("Established database connection.")
    else:
        print("ERROR: Failed to establish database connection.")
        exit

    API_KEY = input("Enter your odds-api key: ")
    
    print("Storing sports in database...")
    store_all_sports()

    print("Storing fixtures in database...")
    store_all_fixtures()


if __name__ == "__main__":
    main()