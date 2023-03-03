import re
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from w3lib.url import url_query_cleaner
import extruct
import numpy
import pandas as pd
import mysql.connector
import itertools
import webbrowser
from imdb import Cinemagoer
import matplotlib
import csv
import sqlite3


def process_links(links):
    for link in links:
        link.url = url_query_cleaner(link.url)
        yield link


class ImdbCrawler(CrawlSpider):
    name = 'imdb'
    allowed_domains = ['www.imdb.com']
    start_urls = ['https://www.imdb.com/']
    rules = (
        Rule(
            LinkExtractor(
                deny=[
                    re.escape('https://www.imdb.com/offsite'),
                    re.escape('https://www.imdb.com/whitelist-offsite'),
                ],
            ),
            process_links=process_links,
            callback='parse_item',
            follow=True
        ),
    )

    def parse_item(self, response):
        return {
            'url': response.url,
            'metadata': extruct.extract(
                response.text,
                response.url,
                syntaxes=['opengraph', 'json-ld']
            ),
        }


def updateDatabase():
    try:
        cursor = con.cursor()
        with open("excel files/all-weeks-countries.csv", "rt", encoding='utf-8') as allCountries:
            data = csv.DictReader(allCountries)
            # TODO remove the first line with the column titles
            # TODO add .lower() ?
            # TODO remove genre from these tables and normalize with a fourth table
            countriesInfo = [(i['country_name'], i['country_iso2'], i['week'], i['category'], i['weekly_rank'],
                              i['show_title'], i['season_title'], i['cumulative_weeks_in_top_10']) for i in data]
            cursor.executemany("INSERT INTO rankByCountry (country_name,country_iso2,week,category,"
                               "weekly_rank,show_title,season_title,cumulative_weeks_in_top_10) VALUES "
                               "(?,?,?,?,?,?,?,?);", countriesInfo)
            allCountries.close()
        with open("excel files/all-weeks-global.csv", "rt", encoding='utf-8') as allGlobal:
            data = csv.DictReader(allGlobal)
            globalInfo = [(i['week'], i['category'], i['weekly_rank'], i['show_title'], i['season_title'],
                           i['weekly_hours_viewed'], i['cumulative_weeks_in_top_10']) for i in data]
            cursor.executemany("INSERT INTO rankByWeek (week,category,weekly_rank,show_title,season_title,"
                               "weekly_hours_viewed,cumulative_weeks_in_top_10) VALUES "
                               "(?,?,?,?,?,?,?);", globalInfo)
            allGlobal.close()
        with open("excel files/most-popular.csv", "rt", encoding='utf-8') as popular:
            data = csv.DictReader(popular)
            popularInfo = [(i['category'], i['rank'], i['show_title'], i['season_title'],
                            i['hours_viewed_first_28_days']) for i in data]
            cursor.executemany("INSERT INTO mostPopular (category,rank,show_title,season_title,"
                               "hours_viewed_first_28_days) VALUES "
                               "(?,?,?,?,?);", popularInfo)
            popular.close()
        df = pd.DataFrame(cursor.execute("SELECT DISTINCT(show_title) FROM rankByCountry"))
        print(df)
    # genreCrawl(i['show_title'])
    # TODO import the excel spreadsheet
    # TODO check most recent entry to see where to start in excel file / sort DB by date?
    # TODO crawl IMDB to get genre information
    except sqlite3.DatabaseError as dbErr:
        print(dbErr)
        con.rollback()
    finally:
        # con.commit()
        cursor.close()


def genreCrawl(title):
    x = 0
    movie = cg.get_movie(cg.search_movie(title.lower())[0].getID())
    genre = movie.get('genres')
    genres = []
    for g in genre:
        genres.append(g.lower())
    return genres


def recommender():
    cursor = con.cursor()
    # TODO collect user input
    # TODO search DB for matching information and run regression
    # TODO make recommendations based on criteria (user specified?)
    cursor.close()


def initialize():
    try:
        cursor = con.cursor()
        cursor.execute("SELECT * FROM rankByCountry")
        cursor.execute("SELECT * FROM rankByWeek")
        cursor.execute("SELECT * FROM mostPopular")
        cursor.execute("SELECT * FROM genreInfo")
    except sqlite3.DatabaseError as dbErr:
        print("Creating tables")
        try:  # TODO update script with extra fields fetched from spider and regenerate tables
            cursor.executescript("""
                       BEGIN;
                       CREATE TABLE IF NOT EXISTS rankByCountry(country_name,country_iso2,week,category,weekly_rank,show_title,season_title,cumulative_weeks_in_top_10);
                       CREATE TABLE IF NOT EXISTS rankByWeek(week,category,weekly_rank,show_title,season_title,weekly_hours_viewed,cumulative_weeks_in_top_10);
                       CREATE TABLE IF NOT EXISTS mostPopular(category,rank,show_title,season_title,hours_viewed_first_28_days);
                       CREATE TABLE IF NOT EXISTS genreInfo(title, genres);
                       COMMIT;""")
        except sqlite3.DatabaseError as dbErr2:
            print(dbErr2)
    finally:
        cursor.close()


def resetDatabase():
    try:
        cursor = con.cursor()
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


con = sqlite3.connect("netflix.db")
cg = Cinemagoer()
crawler = ImdbCrawler()


def main():
    # resetDatabase()
    # initialize()
    cursor = con.cursor()
    # results = cursor.execute("SELECT * FROM rankByCountry")
    # print(cursor.description)
    # print(results.fetchmany(10))
    # mov = cg.search_movie("the matrix")
    # print(mov.getID())
    s = "Pasión de Gavilanes"
    print(s.title())
    print(genreCrawl("Pasión de Gavilanes"))

    # name = list(map(lambda word: word.capitalize(), s.split()))
    # print(" ".join(name).strip())
    # movie = cg.get_movie(cg.search_movie("the matrix")[0].getID())
    # print(movie.get('genres'))
    # url = "https://www.imdb.com/title/tt" + mov[0].getID()
    # webbrowser.open(url, new=2, autoraise=True)
    # movie = cg.get_movie(cg.search_movie("the matrix")[0].)
    updateDatabase()

    # stub = open("excel files/all-weeks-countries.csv", "rt")
    # stub2 = pandas.read_csv("excel files/all-weeks-countries.csv")
    # print(stub2)
    x = max(2, 4)
    print(x)
    cursor.close()
    if con:
        con.close()


main()
