from pymongo import MongoClient
from random import shuffle


def print_title(movie_list):
    for movie in movie_list:
        print(movie['title'])


def get_data(dbh):
    movies = list(dbh.movielist.find({"valid": {"$ne": False}}))

    shuffle(movies)
    print_title("무작위 10개의 영화:", movies[:10])
    print("총 영화 개수:", len(movies))


# 모든 영화의 유효 상태를 일괄 변경
def reset_validity(dbh, valid):
    data = list(dbh.movielist.find())

    for movie in data:
        dbh.movielist.update_one({"_id": movie["_id"]}, {"$set": {"valid": valid}})


def main():
    try:
        # Mongo DB 기본 포트는 다른 작업을 위해 사용될 가능성이 높으므로, 27018 포트를 사용
        mongo_client = MongoClient(host="localhost", port=27018)
        movie_dbh = mongo_client['moviedb']  # 생성할(된) DB 명칭

        get_data(movie_dbh)
        # reset_validity(movie_dbh)

    except Exception as error:

        print("Failed to connecting DB with error: ", error)
        return -1

if __name__ == '__main__':
    main()
