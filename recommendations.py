import sys
from pymongo import MongoClient
from getdistance import totaldistance
from random import sample
import random

genrelist = ['블랙코미디', 'SF', '애니메이션', '느와르', '서스펜스', '모험', '멜로/로맨스', '서사', '코미디', '다큐멘터리', '서부', '무협', '전쟁', '드라마', '스릴러', '에로', '가족', '액션', '공연실황', '공포', '범죄', '판타지', '미스터리', '실험', '뮤지컬']

def genretogenre(dbh):
    #first initialize all the genre items with db
    dbh.genrecounts.delete_many({})
    for genre in genrelist:
        if dbh.genrecounts.find({'type': genre}).count() < 1:
            counter = {}
            for item in genrelist:
                if item != genre:
                    counter[item] = 0
            counter['type'] = genre
            #print("inserting:", counter)
            dbh.genrecounts.insert_one(counter)
            #print("Inserting:", counter)

    for movie in list(dbh.movielist.find({'valid': True})):
        if('genre' not in movie):
            continue
        for x in movie['genre']:
            for y in movie['genre']:
                if x != y:
                    dbh.genrecounts.update_one({'type': x}, {'$inc': {y: 1}})
                    #print("Incrementing", x, "for", y)

def choosegenre(genre, dbh, seen, cands=10):
    thisgenre = dbh.genrecounts.find_one({'type': genre})
    counter = {}
    total = 0

    originalgenre = genre#original genre that is being referenced
    for gen in thisgenre:
        if gen != '_id' and gen != 'type':
            counter[gen] = thisgenre[gen]
            total += counter[gen]
        else:
            continue
    counter[originalgenre] = total * 2 // 3
    total = total + counter[genre]
    keys = list(counter.keys())
    #print(keys)
    ranges = [0]
    for i in range(len(keys)):
        ranges.append(ranges[i] + counter[keys[i]])
        #print(counter[key])

    genrecands = {}
    for key in counter:
        if counter[key] != 0:
            genrecands[key] = []
            #include the original genre among the candidates
            movielist = list(dbh.movielist.find({'valid': True, 'genre': {'$in': [key, originalgenre]}}).sort('rating', -1))
            while(len(genrecands[key]) < cands and movielist != []):
                movie = movielist.pop(0)
                if movie['code'] not in seen:
                    genrecands[key].append(movie)
            random.shuffle(genrecands[key])
    ret = []
    genrespicked = []
    for _ in range(cands):
        val = random.random() * total
        found = 0
        for i in range(len(keys)):
            if ranges[i] < val < ranges[i + 1] and counter[keys[i]] != 0:
                found = i
                ret.append(genrecands[keys[i]].pop())
                genrespicked.append(keys[i])
                break
    return ret

def removemovie(code, checklist):
    i = 0
    while i < len(checklist):
        if(checklist[i]["code"] == code):
            checklist.pop(i)
            return
        i += 1

def findmovies(movieseen, dbh):
    ret = []
    for code in movieseen:
        movie = dbh.movielist.find_one({"code": code})
        if movie:
            ret.append(movie)
    return ret

def addedtotaldistance(movielist, movieb, dbh):
    ret = 0
    for moviea in movielist:
        ret += totaldistance(moviea, movieb, dbh)
    return ret

def main():
    try:
        c = MongoClient(host='localhost', port=27018)
        dbh = c['moviedb']
    except:
        print("Unable to connect!")
        sys.exit(1)
    genretogenre(dbh)
    with open('movieseen.txt', 'r') as movieseen:
        movieseen = movieseen.read().split()

        ret = []
        for movie in movieseen:
            actual_movie = dbh.movielist.find_one({'code': movie})
            if not actual_movie:
                continue
            genres = dbh.movielist.find_one({'code': movie})['genre']
        #    print(genres)
            for genre in genres:
                ret = ret + choosegenre(genre, dbh, movieseen)

        ret = list(set([code['code'] for code in ret]))
        ret.sort(key=lambda x: dbh.movielist.find_one({'code': x})['rating'])
        print("Recommendations based on ratings and genre preference")
        print()
        for movie in ret[:10]:
            print(dbh.movielist.find_one({'code': movie})['title'], dbh.movielist.find_one({'code': movie})['code'])

        print()
        print("Recommendations based on total distance algorithm")

        checklist = list(dbh.movielist.find({"valid": True}))
        # grabbing movielist
        movielist = findmovies(movieseen, dbh)
        # removing seen movies from recommendation list
        for code in movieseen:
            removemovie(code, checklist)
        checklist.sort(key = lambda x: addedtotaldistance(movielist, x, dbh))
        candidates = sample(checklist[:40], 10)
        for candidate in candidates:
            print(candidate["title"], candidate["code"], candidate["rating"])



if __name__ == '__main__':
    main()
