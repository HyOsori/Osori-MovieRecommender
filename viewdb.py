from pymongo import MongoClient
from random import shuffle


def print_title(movie_list):
    for movie in movie_list:
        print(movie['title'])


def get_data(dbh):

    data = list(dbh.movielist.find({"valid": True}))
    shuffle(data)
    print_title(data[:10])
    print(len(data))


# 영화
def reset_validity(dbh):
    data = list(dbh.movielist.find())

    for movie in data:
        dbh.movielist.update_one({"_id": movie["_id"]}, {"$set": {"valid": True}})


def main():
    try:
        # 디비에 연결 포트는 다른 작업을 위해 27018에 함 db 열어줄때 따로 써줘야한다
        mongo_client = MongoClient(host="localhost", port=27018)
        movie_dbh = mongo_client['moviedb']  # 생성할/된 db 이름

        get_data(movie_dbh)
        # reset_validity(movie_dbh)

    except Exception as error:

        print("Failed to connecting DB with error: ", error)
        return -1

if __name__ == '__main__':
    main()
