from bs4 import BeautifulSoup
from pymongo import MongoClient
from random import shuffle

def printtitle(movielist):
    for movie in movielist:
        print(movie['title'])

def getdata(dbh):
    data = list(dbh.movielist.find({"valid": True}))
    shuffle(data)
    printtitle(data[:10])
    print(len(data))

def resetvalidity(dbh):
    data = list(dbh.movielist.find())

    for movie in data:
        dbh.movielist.update_one({"_id": movie["_id"]}, {"$set": {"valid": True}})

def main():
    try:
        c = MongoClient(host="localhost", port=27018)
        dbh = c['moviedb']
        getdata(dbh)
        #resetvalidity(dbh)
    except Exception as e:
        print(str(e))
        print("Error connecting to database!")
        return

if __name__ == '__main__':
    main()
