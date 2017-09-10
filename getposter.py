#-*- coding: utf-8 -*-

import requests, codecs, urllib, re
import urllib.request
from bs4 import BeautifulSoup
from pymongo import MongoClient
from multithreading import multi_threading

def getposter(movie,dbh):
    baseurl = 'http://movie.naver.com/movie/bi/mi/basic.nhn?code='
    moviecode = movie['code']
    code = str(moviecode)

    movieurl = baseurl + code

    url = urllib.request.urlopen(movieurl)
    data = url.read()

    soup = BeautifulSoup(data,'html.parser')

    links = soup.find_all("div",{"class":"poster"})[0]

    for img in links.findAll('img'):
        return img.get('src')

def test():
    baseurl = "http://movie.naver.com/movie/bi/mi/basic.nhn?code="
    code = 146469 # 특정 영화로 test해보는함수..
    urlsum = baseurl + str(code)
    url = urllib.request.urlopen(urlsum)
    data = url.read()

    soup = BeautifulSoup(data,'html.parser')

    links = soup.find_all("div",{"class":"poster"})[0]

    for img in links.findAll('img'):
        print(img.get('src'))

def iteratemovie(dbh):
    data = list(dbh.movielist.find({"valid": True}))
    multithreading(getposter, [[movie, dbh] for movie in data], 20)

def main():

    test()
    ##try:
    ##    c = MongoClient(host="localhost", port=27018)
    ##    dbh = c['moviedb']
    ##    iteratemovie(dbh)

    ##except Exception as inst:
    ##    print(inst)
    ##    print("Error connecting to database!")
    ##    return

if __name__ == '__main__':
    main()
