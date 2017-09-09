import scrapedirectory as scrape_movies
import gettraits as get_tratis
import getgenres as get_genres
import viewdb as show_result

if __name__ == '__main__':

    print("영화 목록 수집 시작")
    scrape_movies.main()

    print("영화 특성 수집 시작")
    get_tratis.main()

    print("영화 장르 수집 시작")
    get_genres.main()

    print("영화 수집 완료")
    show_result.main()
