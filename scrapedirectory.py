#-*- coding: utf-8 -*-

import requests, codecs, urllib, re
import urllib.request
from bs4 import BeautifulSoup
from pymongo import MongoClient

browseurl = 'http://movie.naver.com/movie/sdb/browsing/bmovie.nhn?'
baseurl = 'http://movie.naver.com'

banlist = []

def getparams():
    years = open('years.txt', 'r')
    countries = open('countries.txt', 'r')
    years = years.read().split()
    countries = countries.read().split()
    ret = []
    for year in years:
        for country in countries:
            for page in range(1, 50):
                ret.append(urllib.parse.urlencode((('open', str(year)), ('nation', str(country)), ('page', str(page)))))
    #print(ret)
    return ret

def getmovies(param, dbh):
    url = browseurl + param
    try:
        with urllib.request.urlopen(url) as response:
            htmlcode = response.read().decode(response.headers.get_content_charset(), errors='replace')

            soup = BeautifulSoup(htmlcode, 'html.parser')

            movielist = soup.findAll("ul", {"class": "directory_list"})[0].findAll('a', class_=lambda x: x != 'green')

            movieul = soup.findAll("ul", {"class": "directory_list"})[0]

            for li in movieul.children:
                soup = BeautifulSoup(str(li), 'html.parser')
                movie = soup.findAll('a', class_=lambda x: x != 'green')

                if movie != []:
                    movie = movie[0]
                    title = movie.text
                    link = baseurl + movie['href']
                    code = link[link.index('=') + 1:]
                    movieitem = {'title': title, 'link': link, 'code': code}
                    print("Reviewing movie: " + title)
                    #print(movieitem)
                    if dbh.movielist.find({'code': code}).count() < 1:
                        dbh.movielist.insert_one(movieitem)
                        print("Inserting movie: " + title)
                    elif dbh.movielist.find({'code': code}).count() == 1:
                        #print(soup)
                        seen = False
                        for ban in banlist:
                            if ban in str(soup):
                                seen = True
                                break
                        if seen:
                            dbh.movielist.update_one({'code': code}, {'$set': {'valid': False}})
                            print("Invalid movie!")

            for movie in movielist:
                try:
                    title = movie.text
                    link = baseurl + movie['href']
                    code = link[link.index('=') + 1:]
                    movieitem = {'title': title, 'link': link, 'code': code}
                    if dbh.movielist.find({'code': code}).count() < 1:
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
        c = MongoClient(host="localhost", port=27018)
        dbh = c['moviedb']
    except:
        print("Error connecting to database!")
        return

    params = getparams()
    for param in params:
        getmovies(param, dbh)

if __name__ == '__main__':
    main()
