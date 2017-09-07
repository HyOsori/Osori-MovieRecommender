#-*- coding: utf-8 -*-

import requests, codecs, urllib, re
import urllib.request
from bs4 import BeautifulSoup
from pymongo import MongoClient
from multithreading import multithreading


browseurl = 'http://movie.naver.com/movie/sdb/browsing/bmovie.nhn?'
baseurl = 'http://movie.naver.com'

banlist = ['TV시리즈', 'TV영화', '단편영화', '비디오영화', '옴니버스영화', '뮤직비디오', '옴니버스영화', '웹드라마', '인터넷영화', '공연실황', '뮤지컬']

def getparams():
    years = open('years.txt', 'r')# 연도 목록
    countries = open('countries.txt', 'r')# 나라 목록
    years = years.read().split()# 연도별로 나눠서 만든 리스트
    countries = countries.read().split()
    ret = []
    for year in years:#연도별 나라별로 루프 돌림
        for country in countries:
            for page in range(1, 50):
                ret.append(urllib.parse.urlencode((('open', str(year)), ('nation', str(country)), ('page', str(page)))))#반환할 리스트에 추가
    #print(ret)
    return ret

def getmovies(param, dbh):
    url = browseurl + param#url 합성
    try:#Exception 대비해서 try문 안에 넣음
        with urllib.request.urlopen(url) as response:#with 문 안에 열어서 url 열고 닫는 문제 해결
            htmlcode = response.read().decode(response.headers.get_content_charset(), errors='replace')#한글 해석을 위한 부분

            soup = BeautifulSoup(htmlcode, 'html.parser')#html 파서 initialize

            movielist = soup.findAll("ul", {"class": "directory_list"})[0].findAll('a', class_=lambda x: x != 'green')#ul 요소에서 class 구별해서 스크레이핑

            movieul = soup.findAll("ul", {"class": "directory_list"})[0]#리스트 변수에 저장

            for li in movieul.children:
                soup = BeautifulSoup(str(li), 'html.parser')
                movie = soup.findAll('a', class_=lambda x: x != 'green')

                if movie != []:
                    movie = movie[0]
                    title = movie.text
                    link = baseurl + movie['href']
                    code = link[link.index('=') + 1:]
                    movieitem = {'title': title, 'link': link, 'code': code, 'valid' : True}
                    print("Reviewing movie: " + title)
                    #print(movieitem)
                    if dbh.movielist.find({'code': code}).count() < 1:#디비에 없는 경우
                        dbh.movielist.insert_one(movieitem)#한줄 넣기
                        print("Inserting movie: " + title)
                    elif dbh.movielist.find({'code': code}).count() == 1:#디비에 이미 들어가있는 경우
                        #print(soup)
                        seen = False
                        for ban in banlist:
                            if ban in str(soup):
                                #print(str(soup))
                                seen = True
                                break
                        if seen:
                            dbh.movielist.update_one({'code': code}, {'$set': {'valid': False}})
                            print("Invalid movie!")

            for movie in movielist:
                try:
                    title = movie.text#요소 분석
                    link = baseurl + movie['href']
                    code = link[link.index('=') + 1:]
                    movieitem = {'title': title, 'link': link, 'code': code}#아이템 생성 후 넣기
                    if dbh.movielist.find({'code': code}).count() < 1:#디비에 없는 경우
                        dbh.movielist.insert_one(movieitem)
                        print("Inserting movie: " + title)

                except Exception as inst:
                    print(inst)
                    continue

    except Exception as inst:
        print(inst)
        print("URL NOT FOUND!")

def main():
    try:
        c = MongoClient(host="localhost", port=27018)#디비에 연결 포트는 다른 작업을 위해 27018에 함 db 열어줄때 따로 써줘야한다
        dbh = c['moviedb']#생성할/된 db 이름
    except:
        print("Error connecting to database!")
        return

    params = getparams()

    multithreading(getmovies, [[browseurl + param, dbh] for param in params], 20)
    #멀티 스레딩 함수를 통해 동시에 10개씩 크롤링

if __name__ == '__main__':
    main()
