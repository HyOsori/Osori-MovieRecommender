import re
import urllib.request
import threading
from bs4 import BeautifulSoup
from multithreading import multi_threading
from pymongo import MongoClient

lock = threading.Lock()


def get_genres(movie, dbh):

    movie_url = movie["link"]
    try:
        with urllib.request.urlopen(movie_url) as response:

            # 한글 디코딩을 위한 부분
            html_code = response.read().decode(response.headers.get_content_charset(), errors='replace')

            info_spec = BeautifulSoup(html_code, 'html.parser').find("dl", {"class": "info_spec"})
            new_soup = BeautifulSoup(str(info_spec), 'html.parser').find('dt', {'class': 'step1'})

            if new_soup is not None:

                holder = new_soup.find_next_siblings()[0]
                genre_names = []

                if holder.find('a', {'href': re.compile('.*genre.*')}) is not None:

                    for genre in holder.find_all('a', {'href': re.compile('.*genre.*')}):

                        genre_name = genre.text
                        genre_names.append(genre_name)
                        genre_link = genre.attrs['href']
                        genre_code = genre_link[genre_link.index('=') + 1:]
                        genre_weight = {}

                        genre = {'name': genre_name, 'link': genre_link, 'code': genre_code, 'weight': genre_weight}

                        lock.acquire()
                        if dbh.genrelist.find({'code': genre_code}).count() < 1:

                            for exist_genre in dbh.genrelist.find():

                                genre['weight'][exist_genre['name']] = 0
                                weight = exist_genre['weight']
                                weight[genre_name] = 0
                                dbh.genrelist.update({'name': exist_genre['name']}, {'$set': {'weight': weight}})

                            dbh.genrelist.insert_one(genre)
                        lock.release()

                    dbh.movielist.update({'code': movie['code']}, {'$set': {'genre': genre_names}})

                    for first_genre_name in genre_names:
                        for second_genre_name in genre_names:

                            if first_genre_name != second_genre_name:

                                weight = dbh.genrelist.find({'name': first_genre_name})[0]['weight']
                                weight[second_genre_name] = weight[second_genre_name] + 1

                                dbh.genrelist.update({'name': first_genre_name}, {'$set': {'weight': weight}})

                else:
                    dbh.movielist.update({'_id': movie['_id']}, {'$set': {"valid": False}})
                    print("Not found, Invalid movie!")

                    return -1
            else:
                dbh.movielist.update({'_id': movie['_id']}, {'$set': {"valid": False}})
                print("Not found, Invalid movie!")

                return -1

    except Exception as inst:
        print(inst)


def iterate_movie(dbh):
    movies = list(dbh.movielist.find({"valid": {"$ne": False}}))

    # 멀티 스레드를 이용해 동시에 20개씩 크롤링
    multi_threading(get_genres, [[movie, dbh] for movie in movies], 20)


def main():
    try:
        # Mongo DB 기본 포트는 다른 작업을 위해 사용될 가능성이 높으므로, 27018 포트를 사용
        mongo_client = MongoClient(host="localhost", port=27018)
        movie_dbh = mongo_client['moviedb']  # 생성할(된) DB 명칭

        iterate_movie(movie_dbh)

    except Exception as error:
        print("Error: ", error)
        return -1

if __name__ == '__main__':
    main()
