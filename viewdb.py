from pymongo import MongoClient
from random import shuffle
import re

def print_title(movie_list, dbh):
    for movie in movie_list:
        view_movie(dbh, movie["code"])



def get_data(dbh):
    movies = list(dbh.movielist.find({"valid": {"$ne": False}}))
    all_movies = list(dbh.movielist.find({}))
    shuffle(movies)
    print_title(movies[:10], dbh)
    print("총 영화 갯수:", len(all_movies))
    print("사용 가능 영화 갯수:", len(movies))


# 모든 영화의 유효 상태를 일괄 변경
def reset_validity(dbh, valid):
    data = list(dbh.movielist.find())

    for movie in data:
        dbh.movielist.update_one({"_id": movie["_id"]}, {"$set": {"valid": valid}})

def view_movie(dbh, code):
    movie = dbh.movielist.find_one({"code": code})
    print(movie["title"])
    print(movie["genre"])
    print(movie["rating"])

def find_by_title(dbh, title):
    pattern = re.compile(title, re.I)
    result = dbh.movielist.find({"title": {"$regex": pattern}})
    print(result)

def disable_incorrect_movies(dbh):
    movies = list(dbh.movielist.find({"valid": {"$ne": False}}))
    count = 0
    for movie in movies:
        if('genre' not in movie):
            dbh.movielist.update_one({"_id": movie["_id"]}, {"$set": {"valid": False}})
            count += 1
        elif('공연실황' in movie['genre']):
            dbh.movielist.update_one({"_id": movie["_id"]}, {"$set": {"valid": False}})
        if('rating' not in movie):
            dbh.movielist.update_one({"_id": movie["_id"]}, {"$set": {"valid": False}})
            #print(movie["title"])
            count += 1
    print(count)

def main():
    try:
        # Mongo DB 기본 포트는 다른 작업을 위해 사용될 가능성이 높으므로, 27018 포트를 사용
        mongo_client = MongoClient(host="localhost", port=27018)
        dbh = mongo_client['moviedb']  # 생성할(된) DB 명칭

        disable_incorrect_movies(dbh)
        get_data(dbh)
        view_movie(dbh, "146469")
        #reset_validity(dbh, True)

    except Exception as error:

        print("Failed to connecting DB with error: ", error)
        return -1

if __name__ == '__main__':
    main()
