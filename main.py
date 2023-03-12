import re
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from w3lib.url import url_query_cleaner
import extruct
import numpy
import pandas as pd
import itertools
import webbrowser
from imdb import Cinemagoer
from multipledispatch import dispatch
import matplotlib
import datetime
from timeit import default_timer
import sys
import csv
import sqlite3


def updateDatabase():
    try:
        cursor = con.cursor()
        # generate the first three databases from csv files
        with open("excel files/all-weeks-countries.csv", "rt", encoding='utf-8') as allCountries:
            data = csv.DictReader(allCountries)
            # TODO add .lower() ?
            countriesInfo = [(i['country_name'], i['country_iso2'], i['week'], i['category'], i['weekly_rank'],
                              i['show_title'], i['season_title'], i['cumulative_weeks_in_top_10']) for i in data]
            cursor.executemany("INSERT OR IGNORE INTO rankByCountry (country_name,country_iso2,week,category,"
                               "weekly_rank,show_title,season_title,cumulative_weeks_in_top_10) VALUES "
                               "(?,?,?,?,?,?,?,?);", countriesInfo)
            allCountries.close()
        with open("excel files/all-weeks-global.csv", "rt", encoding='utf-8') as allGlobal:
            data = csv.DictReader(allGlobal)
            globalInfo = [(i['week'], i['category'], i['weekly_rank'], i['show_title'], i['season_title'],
                           i['weekly_hours_viewed'], i['cumulative_weeks_in_top_10']) for i in data]
            cursor.executemany("INSERT OR IGNORE INTO rankByWeek (week,category,weekly_rank,show_title,season_title,"
                               "weekly_hours_viewed,cumulative_weeks_in_top_10) VALUES "
                               "(?,?,?,?,?,?,?);", globalInfo)
            allGlobal.close()
        with open("excel files/most-popular.csv", "rt", encoding='utf-8') as popular:
            data = csv.DictReader(popular)
            popularInfo = [(i['category'], i['rank'], i['show_title'], i['season_title'],
                            i['hours_viewed_first_28_days']) for i in data]
            cursor.executemany("INSERT OR IGNORE INTO mostPopular (category,rank,show_title,season_title,"
                               "hours_viewed_first_28_days) VALUES "
                               "(?,?,?,?,?);", popularInfo)
            popular.close()
        # generate the genre table by using existing tables and imdb library
        df = pd.DataFrame(cursor.execute("SELECT DISTINCT show_title FROM rankByCountry "
                                         "LEFT JOIN genreInfo ON rankByCountry.show_title = genreInfo.title "
                                         "WHERE genres IS NULL LIMIT 5"))
        if df.values.size > 0:  # check if genre table even needs to be updated
            earliestYear = int(pd.DataFrame(cursor.execute("SELECT week FROM rankByCountry WHERE season_title != '' "
                                                           "ORDER BY week ASC LIMIT 1"))[0].values[0].split('-')[0])
            latestYear = int(pd.DataFrame(cursor.execute("SELECT week FROM rankByCountry WHERE season_title != '' "
                                                         "ORDER BY week DESC LIMIT 1"))[0].values[0].split('-')[0])
            while earliestYear <= latestYear:
                # update movies
                df = pd.DataFrame(cursor.execute(f"SELECT DISTINCT show_title FROM "
                                                 f"(SELECT * FROM rankByCountry LEFT JOIN genreInfo ON show_title = title "
                                                 f"WHERE genres IS NULL) WHERE week LIKE '{earliestYear}%' AND season_title = ''"))
                df = df.rename(columns={0: 'title'})
                df['genres'] = df.apply((lambda key: genreCrawl(key.title, earliestYear, False)), axis=1)
                # df.to_sql('genreInfo', con, if_exists='append', index=False)
                # update tv shows
                df = pd.DataFrame(cursor.execute(f"SELECT DISTINCT show_title FROM "
                                                 f"(SELECT * FROM rankByCountry LEFT JOIN genreInfo ON show_title = title "
                                                 f"WHERE genres IS NULL) WHERE week LIKE '{earliestYear}%' AND season_title != ''"))
                df = df.rename(columns={0: 'title'})
                df['genres'] = df.apply((lambda key: genreCrawl(key.title, earliestYear, True)), axis=1)
                # df.to_sql('genreInfo', con, if_exists='append', index=False)
                earliestYear += 1
        print(pd.DataFrame(cursor.execute("SELECT * FROM genreInfo")))
        # df.drop(columns={'week', "season"}, inplace=True)
        print(pd.DataFrame(cursor.execute("SELECT * FROM genreInfo")))
        args = ['country_name', 'week']  # for creating variable search queries from user selected categories,  not for this module
        query = f"SELECT {(lambda x: ', '.join(x))(args)} FROM rankByCountry ORDER BY show_title DESC LIMIT 50"
        df = pd.DataFrame(cursor.execute(query))
        print("\n", df.info)
        df.rename(mapper=lambda x: args[x], axis=1, inplace=True)  # using the args list to rename columns
        entry1 = df.at[0, 'week'].split('-')
        entry2 = df.at[3, 'week'].split('-')
        print(entry1, "\n", entry2)
    # TODO check most recent entry to see where to start in excel file / sort DB by date?
    except sqlite3.DatabaseError as dbErr:
        print(dbErr)
        con.rollback()
    finally:
        # con.commit()
        cursor.close()


def genreCrawl(title, year, season):  # TODO make this function smarter than selecting the first result
    movieID = None
    try:
        movieList = cg.search_movie(title.lower())
        movieList = list(filter(lambda x: len(x.data['title']) == len(title), movieList))
        closestYear = sys.maxsize
        if season:
            movieList = list(filter(lambda x:  len(x.data['title']) == len(title) and (x.data['kind'] == 'tv series'), movieList))
            mList = []
            for m in movieList:  # bias towards shows with more seasons
                mList.append(cg.get_movie(m.getID()))
            for m in mList:
                seasons = len(m.data['seasons']) - 1
                topSeasons = 0
                if year - (m['year'] + seasons) < closestYear:
                    movieID = m.getID()
                    closestYear = year - m['year']
                elif year - (m['year'] + seasons) == closestYear:
                    if seasons > topSeasons:
                        topSeasons = seasons
                        movieID = m.getID()
                        closestYear = year - (m['year'] + seasons)
        else:
            movieList = list(filter(lambda x:  len(x.data['title']) == len(title) and (x.data['kind'] == 'movie'), movieList))
            for m in movieList:
                if (year - m['year']) < closestYear:
                    movieID = m.getID()
                    closestYear = year - m['year']
    except KeyError as kErr:
        print(kErr)
        print(movieID)
    finally:
        movie = cg.get_movie(movieID)
        genre = movie.get('genres')
        genres = []
        for g in genre:
            genres.append(g.lower())
        return str(genres)


def filterDate(csvFile):  # TODO compare existing database most recent date to csv file dates
    cursor = con.cursor()


def recommender():
    cursor = con.cursor()
    # TODO collect user input
    # TODO search DB for matching information and run regression
    # TODO make recommendations based on criteria (user specified?)
    cursor.close()


def initialize():
    cursor = con.cursor()
    try:
        cursor.execute("SELECT * FROM rankByCountry")
        cursor.execute("SELECT * FROM rankByWeek")
        cursor.execute("SELECT * FROM mostPopular")
        cursor.execute("SELECT * FROM genreInfo")
    except sqlite3.DatabaseError as dbErr:
        print("Creating tables")
        try:
            cursor.executescript("""
                       BEGIN;
                       CREATE TABLE IF NOT EXISTS rankByCountry(country_name,country_iso2,week,category,weekly_rank,show_title,season_title,cumulative_weeks_in_top_10,
                       UNIQUE (country_name,country_iso2,week,category,weekly_rank,show_title,season_title,cumulative_weeks_in_top_10));
                       CREATE TABLE IF NOT EXISTS rankByWeek(week,category,weekly_rank,show_title,season_title,weekly_hours_viewed,cumulative_weeks_in_top_10,
                       UNIQUE (week,category,weekly_rank,show_title,season_title,weekly_hours_viewed,cumulative_weeks_in_top_10));
                       CREATE TABLE IF NOT EXISTS mostPopular(category,rank,show_title,season_title,hours_viewed_first_28_days,
                       UNIQUE (category,rank,show_title,season_title,hours_viewed_first_28_days));
                       CREATE TABLE IF NOT EXISTS genreInfo(title, genres);
                       COMMIT;""")
        except sqlite3.DatabaseError as dbErr2:
            print(dbErr2)
    finally:
        cursor.close()


def resetDatabase():
    cursor = con.cursor()
    try:
        cursor.executescript(""
                             "BEGIN;"
                             "DROP TABLE IF EXISTS rankByCountry;"
                             "DROP TABLE IF EXISTS rankByWeek;"
                             "DROP TABLE IF EXISTS mostPopular;"
                             "DROP TABLE IF EXISTS genreInfo;"
                             "COMMIT;")
    except sqlite3.DatabaseError as dbErr:
        print(dbErr)
    finally:
        cursor.close()
        con.commit()


sys.stdout.reconfigure(encoding='utf-8')
con = sqlite3.connect("netflix.db")
cg = Cinemagoer()


def main():
    resetDatabase()
    # initialize()
    cursor = con.cursor()
    # results = cursor.execute("SELECT * FROM rankByCountry")
    # print(cursor.description)
    # print(results.fetchmany(10))
    # mov = cg.search_movie("the matrix")
    # print(mov.getID())
    # print(genreCrawl("Pasión de Gavilanes"))

    # name = list(map(lambda word: word.capitalize(), s.split()))
    # print(" ".join(name).strip())
    # movie = cg.get_movie(cg.search_movie("the matrix")[0].getID())
    # print(movie.get('genres'))
    # url = "https://www.imdb.com/title/tt" + mov[0].getID()
    # webbrowser.open(url, new=2, autoraise=True)
    # updateDatabase()
    # df = pd.DataFrame(cursor.execute("SELECT DISTINCT(show_title), season_title FROM rankByCountry WHERE season_title != '' ORDER BY show_title"))
    # df2 = pd.DataFrame(cursor.execute("SELECT DISTINCT(show_title), season_title FROM rankByCountry WHERE season_title = '' ORDER BY show_title"))
    # df3 = pd.DataFrame(cursor.execute("SELECT DISTINCT(show_title), season_title FROM rankByCountry ORDER BY show_title"))
    # df4 = pd.DataFrame(cursor.execute("SELECT DISTINCT(show_title) FROM rankByCountry ORDER BY show_title"))
    # df5 = pd.DataFrame(cursor.execute("SELECT DISTINCT(show_title), week, season_title FROM rankByCountry "
    #                                 "LEFT JOIN genreInfo ON rankByCountry.show_title = genreInfo.title "
    #                                 "WHERE genres IS NULL ORDER BY show_title"))
    # df6 = pd.DataFrame(cursor.execute("SELECT DISTINCT(show_title), week FROM rankByCountry "
    #                                 "LEFT JOIN genreInfo ON rankByCountry.show_title = genreInfo.title "
    #                                 "WHERE genres IS NULL ORDER BY show_title"))
    cursor.close()
    if con:
        con.close()


main()
