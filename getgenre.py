
import requests, codecs, urllib, re
import urllib.request
import threading
from bs4 import BeautifulSoup
from multithreading import multithreading

lock = threading.Lock()

def getGenre(movie, dbh):
    movie_url = movie["link"]
    try:
        with urllib.request.urlopen(movie_url) as response:
            htmlcode = response.read().decode(response.headers.get_content_charset(), errors='replace')  # 한글 해석을 위한 부분

            infospec = BeautifulSoup(htmlcode, 'html.parser').find("dl", {"class": "info_spec"})
            newsoup = BeautifulSoup(str(infospec), 'html.parser').find('dt', {'class': 'step1'})

            if newsoup != None:
                holder = newsoup.find_next_siblings()[0]
                genrenamelist = []

                if holder.find('a', {'href':re.compile('.*genre.*')}) != None :
                    for genre in holder.find_all('a', {'href': re.compile('.*genre.*')}):
                        genrename = genre.text
                        genrenamelist.append(genrename)
                        genrelink = genre.attrs['href']
                        genrecode = genrelink[genrelink.index('=') + 1:]
                        genreweight = {}
                        genreitem = {'name': genrename, 'link': genrelink, 'code': genrecode, 'weight': genreweight}

                        lock.acquire()
                        if dbh.genrelist.find({'code': genrecode}).count() < 1:
                            for existgenre in dbh.genrelist.find():
                                genreitem['weight'][existgenre['name']] = 0
                                tmpweight = existgenre['weight']
                                tmpweight[genrename] = 0
                                dbh.genrelist.update({'name': existgenre['name']}, {'$set': {'weight': tmpweight}})
                            dbh.genrelist.insert_one(genreitem)
                        lock.release()

                    dbh.movielist.update({'code':movie['code']}, {'$set': {'genre' : genrenamelist}})

                    for first_genrename in genrenamelist:
                        for second_genrename in genrenamelist:
                            if first_genrename != second_genrename:
                                tmpweight = dbh.genrelist.find({'name': first_genrename})[0]['weight']
                                tmpweight[second_genrename] = tmpweight[second_genrename] + 1
                                dbh.genrelist.update({'name': first_genrename}, {'$set': {'weight': tmpweight}})

                else:
                    dbh.movielist.update({'_id': movie['_id']}, {'$set': {"valid": False}})
                    print("Invalid movie!")
                    print("Not found!")
                    return
            else:
                dbh.movielist.update({'_id': movie['_id']}, {'$set': {"valid": False}})
                print("Invalid movie!")
                print("Not found!")
                return

    except Exception as inst:
            print(inst)

def iteratemovie(dbh):
    data = list(dbh.movielist.find({"valid": True}))
    multithreading(getGenre, [[movie, dbh] for movie in data], 20)

def main():
    try:
        c = MongoClient(host="localhost", port=27018)
        dbh = c['moviedb']
        iteratemovie(dbh)
    except Exception as inst:
        print(inst)
        print("Error connecting to database!")
        return

if __name__ == '__main__':
    main()
