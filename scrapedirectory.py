# -*- coding: utf-8 -*-

import urllib.request
from bs4 import BeautifulSoup
from pymongo import MongoClient
from multithreading import multi_threading


BROWSE_URL = 'http://movie.naver.com/movie/sdb/browsing/bmovie.nhn?'
BASE_URL = 'http://movie.naver.com'

BANNED_LIST = ['TV시리즈', 'TV영화', '단편영화', '비디오영화', '옴니버스영화', '뮤직비디오',
               '옴니버스영화', '웹드라마', '인터넷영화', '공연실황', '뮤지컬']


# 추출할 연도와 국가를 조합해 url parameter로 만들어 반환
def get_params():

    years = open('years.txt', 'r')
    years = years.read().split()  # 연도 별로 나눠서 만든 리스트

    countries = open('countries.txt', 'r')
    countries = countries.read().split()  # 국가 별로 나눠서 만든 리스트

    params = []
    for year in years:  # 연도별
        for country in countries:  # 국가별
            for page in range(1, 50):  # 1 ~ 50 페이지 가량을 스크랩핑

                # 연도와 국가, 페이지에 맞는 url parameter를 생성해 추가
                params.append(urllib.parse.urlencode((('open', str(year)), ('nation', str(country)), ('page', str(page)))))

    # print(params)  # FOR DEBUG
    return params


def get_movies(param, dbh):

    url = BROWSE_URL + param  # 전체 url

    # Connection 오류 발생에 대비해 예외처리

    with urllib.request.urlopen(url) as response:  # with 문 안에 열어서 url을 열고 닫는 문제 해결
        try:
            # 한글 디코딩을 위한 부분
            html_code = response.read().decode(response.headers.get_content_charset(), errors='replace')

            # html parser 초기화
            soup = BeautifulSoup(html_code, 'html.parser')

            # ul 요소에서 class로 구별해서 스크랩핑 1
            movie_ul = soup.findAll("ul", {"class": "directory_list"})[0]
            for li in movie_ul.children:
                soup = BeautifulSoup(str(li), 'html.parser')
                parsed_movies = soup.findAll('a', class_=lambda x: x != 'green')

                if not parsed_movies == []:
                    title = parsed_movies[0].text
                    link = BASE_URL + parsed_movies[0]['href']
                    code = link[link.index('=') + 1:]
                    movie_info = {'title': title, 'link': link, 'code': code, 'valid': True}

                    if dbh.movielist.find({'code': code}).count() < 1:  # DB에 없는 경우
                        dbh.movielist.insert_one(movie_info)  # 영화 정보를 삽입
                        print("Insert into DB: ", title)
                    elif dbh.movielist.find({'code': code}).count() == 1:  # DB에 이미 존재하는 경우
                        
                        seen = False
                        for banned_movie in BANNED_LIST:
                            if banned_movie in str(soup):
                                seen = True
                                break
                        
                        if seen:  # 제외 리스트에 포함된 영화 일 경우, vaild -> False
                            dbh.movielist.update_one({'code': code}, {'$set': {'valid': False}})
                            print("Invalid movie: ", title)
                        else:  # 그렇지 않은 경우 영화 정보 업데이트
                            print("Update from DB: ", title)

            # ul 요소에서 class로 구별해서 스크랩핑 2
            movie_list = soup.findAll("ul", {"class": "directory_list"})[0].findAll('a', class_=lambda x: x != 'green')
            for movie_info in movie_list:

                # 예외가 발생하더라도 모든 영화를 탐색해보아야 하기 때문에, 새로운 try ~ except 구문으로 처리
                try:
                    title = movie_info.text  # 요소 분석
                    link = BASE_URL + movie_info['href']
                    code = link[link.index('=') + 1:]

                    movie_info = {'title': title, 'link': link, 'code': code}  # 아이템 생성 후 넣기
                    if dbh.movielist.find({'code': code}).count() < 1:  # 디비에 없는 경우

                        print("Insert into DB: ", title)
                        dbh.movielist.insert_one(movie_info)

                except Exception as error:
                    print("Error1: ", error)
                    continue

        except Exception as error:
            print("Error2: ", error)


def main():

    try:
        # 디비에 연결 포트는 다른 작업을 위해 27018에 함 db 열어줄때 따로 써줘야한다
        mongo_client = MongoClient(host="localhost", port=27018)
        movie_dbh = mongo_client['moviedb']  # 생성할/된 db 이름

    except Exception as error:
        print("Error: ", error)
        return -1

    params = get_params()

    # 멀티 스레드를 이용해 동시에 10개씩 크롤링
    multi_threading(get_movies, [[BROWSE_URL + param, movie_dbh] for param in params], 20)

if __name__ == '__main__':
    main()
