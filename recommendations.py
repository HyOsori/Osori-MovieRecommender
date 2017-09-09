import sys
from pymongo import MongoClient

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
            dbh.genrecounts.insert_one(counter)
            print("Inserting:", counter)

    for movie in list(dbh.movielist.find({'valid': True})):
        for x in movie['genre']:
            for y in movie['genre']:
                if x != y:
                    dbh.genrecounts.update_one({'type': x}, {'$inc': {y: 1}})
                    print("Incrementing", x, "for", y)


def main():
    try:
        c = MongoClient(host='localhost', port=27018)
        dbh = c['moviedb']
    except:
        print("Unable to connect!")
        sys.exit(1)
    genretogenre(dbh)

if __name__ == '__main__':
    main()
