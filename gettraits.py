#-*- coding: utf-8 -*-

import requests, codecs, urllib, re
import urllib.request
from bs4 import BeautifulSoup
from pymongo import MongoClient
from multithreading import multithreading

baseurl = 'http://movie.naver.com'

def gettraits(movie, dbh):

    moviecode = movie["code"]
    movie_url = movie["link"]#urllib.parse.urlencode( (('code', str(moviecode))) )
    print("Connecting to link: " + movie_url)

    try:
        with urllib.request.urlopen(movie_url) as response:
            #htmlcode = response.read().decode('utf8', errors='replace')
            htmlcode = response.read().decode("utf-8", "ignore")#bytes(response.read(), "utf-8").decode("utf-8", "ignore")

            htmlcode = str(htmlcode.encode("utf-8"))
            #print(htmlcode)

            soup = BeautifulSoup(htmlcode, 'html.parser')

            audrating = 0#ratings and raters
            netrating = 0
            audraters = 0
            netraters = 0
            finalrating = 0
            finalraters = 0

            scoreholder = soup.find_all('a', class_='ntz_score')
            #print(soup)

            if scoreholder != []:
                ratingsoup = BeautifulSoup(str(scoreholder[0]), 'html.parser')
                widthcode = ratingsoup.find('span', class_='st_on')['style']
                audrating = float(widthcode[widthcode.index(':') + 1: widthcode.index('%')])
            else:
                audrating = 0

            netscoreholder = soup.findAll('a', {'id': 'pointNetizenPersentBasic'})
            if netscoreholder != None:
                ratingsoup = BeautifulSoup(str(netscoreholder), 'html.parser')
                widthcode = ratingsoup.find('span', class_='st_on')['style']
                netrating = float(widthcode[widthcode.index(':') + 1: widthcode.index('%')])

            if soup.find('div', {'id': 'actualPointCountWide'}) != None:
                audraters = int(BeautifulSoup(str(soup.find('div', {'id': 'actualPointCountWide'})), 'html.parser').find('em').text.replace(',', ''))


            if soup.find('div', {'id': 'pointNetizenCountBasic'}) != None:
                netraters = int(BeautifulSoup(str(soup.find('div', {'id': 'pointNetizenCountBasic'})), 'html.parser').find('em').text.replace(',', ''))

            if netraters > audraters:
                finalrating = netrating
                finalraters = netraters
            else:
                finalrating = audrating
                finalraters = audraters

            validity = True
            if(finalraters < 75):
                validity = False
            print("Scraping movie: " + movie["title"] + " " + \
                str(finalrating) + " " + str(validity))

            dbh.movielist.update_one({"_id": movie["_id"]}, {"$set": {"valid": validity, "raters": finalraters, "rating": finalrating}})
            #print(soup)dnd

    except Exception as inst:
        print(inst)

def iteratemovie(dbh):
    data = list(dbh.movielist.find({"valid": True}))
    multithreading(gettraits, [[movie, dbh] for movie in data], 20)


def main():
    try:
        c = MongoClient(host="localhost", port=27018)#디비에 연결 포트는 다른 작업을 위해 27018에 함 db 열어줄때 따로 써줘야한다
        dbh = c['moviedb']#생성할/된 db 이름
        iteratemovie(dbh)
    except Exception as inst:
        print(inst)
        print("Error connecting to database!")
        return

if __name__ == '__main__':
    main()
