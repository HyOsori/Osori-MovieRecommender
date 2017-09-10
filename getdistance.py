from pymongo import MongoClient
import sys

def directordistance(a, b, dbh):
    if a['director'] == b['director']:
        return 0
    else:
        return 1

def actordistance(a, b, dbh):
    return 1

def comparegenre(a, b, dbh):#get genre b's distance from a
    total = 0
    mindist = 100
    refgenre = dbh.genrecounts.find_one({'type': a})
    for key in refgenre:
        if key != '_id' and key != 'type':
            total += refgenre[key]
    if refgenre[b] != 0:
        mindist = refgenre[b] / total
#    print(a, b, 1 / (mindist * 100))
    return 5 / (mindist * 100)#return the percentage of ratio

def genredistance(a, b, dbh):#movie A movie B
    totaldist = 0
    try:
        for genre in b['genre']:
            totaldistance = 0
            count = 0
            if genre not in a['genre']:
                for ref in a['genre']:
                    totaldistance += comparegenre(ref, genre, dbh)#genre a to genre b
                    count += 1
            if count != 0:
                totaldist += (totaldistance / count)
    except:
        return 100000
    return totaldist

def getratingdistance(a, dbh):

    return 30 / a['rating']

def totaldistance(a, b, dbh):
    return genredistance(a, b, dbh) + getratingdistance(b, dbh)

def main():
    try:
        c = MongoClient(host='localhost', port=27018)
        dbh = c['moviedb']
    except:
        print("Failed to connect to database!")
    comparegenre('드라마', '멜로/로맨스', dbh)
    genredistance(dbh.movielist.find_one({'code': '74866'}), dbh.movielist.find_one({'code': '142822'}), dbh)
    checklist = list(dbh.movielist.find({'valid': True, 'code': {'$ne': '127459'}}))
    moviea = dbh.movielist.find_one({'code': '127459'})
    checklist.sort(key=lambda x: totaldistance(moviea, x, dbh))
    for movie in checklist[:20]:
        print(movie['title'])


if __name__ == '__main__':
    main()
