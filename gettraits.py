# -*- coding: utf-8 -*-

import urllib.request
from bs4 import BeautifulSoup
from pymongo import MongoClient
from multithreading import multi_threading

# 상세영화 정보페이지 기본 URL
BASE_URL = 'http://movie.naver.com/movie/bi/mi/detail.nhn?code='


def get_mainactors(movie, dbh):

    print("Retrieving main actors.")
    try:
        main_actors = []
        with urllib.request.urlopen(BASE_URL + movie['code']) as page:

            page = page.read().decode(page.headers.get_content_charset(), errors='replace')
            soup = BeautifulSoup(page, 'html.parser')

            actors = soup.findAll('div', {'class': 'p_info'})
            for actor in actors:

                if '주연' in str(actor):

                    name = actor.find('a').text
                    link = BASE_URL + actor.find('a')['href']
                    base_link = actor.find('a')['href']
                    code = base_link[base_link.index('=') + 1:]
                    actor_info = {'name': name, 'link': link, 'code': code}
                    print("Actor: " + name + " " + code)

                    main_actors.append(actor_info)

                    if dbh.actorlist.find({'code': code}).count() < 1:

                        dbh.actorlist.insert_one(actor_info)
                        print("Inserting:", name)

            return main_actors

    except Exception as error:
        print("Invalid movie: ", error)
        return -1

def getgenres(movie, dbh):
    baseurl = 'http://movie.naver.com/movie/bi/mi/basic.nhn?code='
    ret = []
    with urllib.request.urlopen(baseurl + movie['code']) as response:
        htmlcode = response.read().decode(response.headers.get_content_charset(), errors='replace')
        soup = BeautifulSoup(htmlcode, 'html.parser')
        #print(soup.prettify())
        links = soup.findAll('a')
        for link in links:
            try:
                if 'movie/sdb/browsing/bmovie.nhn?genre=' in str(link):
                    ret.append(link.text)

            except Exception as inst:
                print(inst)
                print("Error with something!")
                continue
        if ret == []:
            dbh.movielist.update_one({'_id': movie['_id']}, {'$set': {'valid': False}})
        for genre in ret:
            dbh.movielist.update_one({'_id': movie['_id']}, {'$addToSet': {'genre': genre}})
        print(ret)


def get_traits(movie, dbh):

    # movie_code = movie["code"]
    # urllib.parse.urlencode(('code', str(movie_code)))

    movie_url = movie["link"]
    print("Connecting to link: " + movie_url)

    with urllib.request.urlopen(movie_url) as response:
        try:
            # html_code = response.read().decode('utf8', errors='replace')
            html_code = response.read().decode("utf-8", "ignore")
            # bytes(response.read(), "utf-8").decode("utf-8", "ignore")
            html_code = str(html_code.encode("utf-8"))
            # print(html_code)

            soup = BeautifulSoup(html_code, 'html.parser')

            aud_rating = 0  # ratings and raters
            net_rating = 0
            aud_raters = 0
            net_raters = 0
            final_rating = 0
            final_raters = 0

            score_holder = soup.find_all('a', class_='ntz_score')
            # print(soup)

            if not score_holder == []:
                rating_soup = BeautifulSoup(str(score_holder[0]), 'html.parser')
                width_code = rating_soup.find('span', class_='st_on')['style']
                aud_rating = float(width_code[width_code.index(':') + 1: width_code.index('%')])
            else:
                aud_rating = 0

            net_score_holder = soup.findAll('a', {'id': 'pointNetizenPersentBasic'})
            if net_score_holder is not None:
                rating_soup = BeautifulSoup(str(net_score_holder), 'html.parser')
                width_code = rating_soup.find('span', class_='st_on')['style']
                net_rating = float(width_code[width_code.index(':') + 1: width_code.index('%')])

            if soup.find('div', {'id': 'actualPointCountWide'}) is not None:
                aud_raters = int(BeautifulSoup(str(soup.find('div', {'id': 'actualPointCountWide'})), 'html.parser').
                                 find('em').text.replace(',', ''))

            if soup.find('div', {'id': 'pointNetizenCountBasic'}) is not None:
                net_raters = int(BeautifulSoup(str(soup.find('div', {'id': 'pointNetizenCountBasic'})), 'html.parser').
                                 find('em').text.replace(',', ''))

            if net_raters > aud_raters:
                final_rating = net_rating
                final_raters = net_raters
            else:
                final_rating = aud_rating
                final_raters = aud_raters

            validity = True
            if final_raters < 75:
                validity = False

            director = ''
            info_spec = soup.find("dl", {"class": "info_spec"})

            new_soup = BeautifulSoup(str(info_spec), 'html.parser')

            director = new_soup.find('dt', {'class': 'step2'})

            # retrieve the director
            if director is not None:

                holder = director.find_next_siblings()[0]
                if holder.find('a') is not None:

                    director_name = holder.find('a').text
                    director_link = holder.find('a')['href']
                    director_code = director_link[director_link.index('=') + 1:]
                    director_item = {'name': director_name, 'link': director_link, 'code': director_code}

                    if dbh.directorlist.find({'code': director_code}).count() < 1:
                        dbh.directorlist.insert_one(director_item)

                    dbh.movielist.update({'_id': movie['_id']}, {'$set': {'director': director_item}})
                    print(director_name, director_code)

                else:
                    dbh.movielist.update({'_id': movie['_id']}, {'$set': {"valid": False}})
                    print("Not found, Invalid movie!")

                    return -1
            else:
                dbh.movielist.update({'_id': movie['_id']}, {'$set': {"valid": False}})
                print("Not found, Invalid movie!")

                return -1

            getgenres(movie, dbh)
            main_actors = get_mainactors(movie, dbh)
            if main_actors:
                dbh.movielist.update({'_id': movie['_id']}, {'$set': {'actors': get_mainactors(movie, dbh)}})
            else:
                dbh.movielist.update({'_id': movie['_id']}, {'$set': {'valid': False}})
                print("Invalid movie!")
                return -1

            print("Scraping movie: " + movie["title"] + " " + str(final_rating) + " " + str(validity))

            dbh.movielist.update_one(
                {
                    "_id": movie["_id"]
                },
                {
                    "$set": {
                        "valid": validity,
                        "raters": final_raters,
                        "rating": final_rating
                    }
                }
            )

        except Exception as error:
            print(error)


def iterate_movie(dbh):
    valid_movies = list(dbh.movielist.find({"valid": True}))
    print(len(valid_movies))

    # 멀티 스레드를 이용해 동시에 20개씩 크롤링
    multi_threading(get_traits, [[movie, dbh] for movie in valid_movies], 20)


def main():
    try:
        # Mongo DB 기본 포트는 다른 작업을 위해 사용될 가능성이 높으므로, 27018 포트를 사용
        mongo_client = MongoClient(host="localhost", port=27018)
        movie_dbh = mongo_client['moviedb']  # 생성할(된) DB 명칭

        iterate_movie(movie_dbh)

    except Exception as error:
        print("Failed to connecting DB with error: ", error)
        return -1

if __name__ == '__main__':
    main()
