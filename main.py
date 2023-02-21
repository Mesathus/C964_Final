import re
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from w3lib.url import url_query_cleaner
import extruct
import numpy
import pandas
import mysql.connector
import itertools
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
        with open("excel files/all-weeks-countries.csv") as allCountries:
            data = csv.DictReader(allCountries)
            countriesInfo = [(i['country_name'], i['country_iso2'], i['week'], i['category'], i['weekly_rank'],
                             i['show_title'], i['season_title'], i['cumulative_weeks_in_top_10']) for i in data]
            cursor.executemany("INSERT INTO rankByCountry (country_name,country_iso2,week,category,"
                               "weekly_rank,show_title,season_title,cumulative_weeks_in_top_10) VALUES "
                               "(?,?,?,?,?,?,?,?);", countriesInfo)
            allCountries.close()
        with open("excel files/all-weeks-global.csv") as allGlobal:
            data = csv.DictReader(allGlobal)
            globalInfo = [(i['week'], i['category'], i['weekly_rank'], i['show_title'], i['season_title'],
                           i['weekly_hours_viewed'], i['cumulative_weeks_in_top_10']) for i in data]
            cursor.executemany("INSERT INTO rankByCountry (week,category,weekly_rank,show_title,season_title,"
                               "weekly_hours_viewed,cumulative_weeks_in_top_10) VALUES "
                               "(?,?,?,?,?,?,?);", globalInfo)
            allGlobal.close()
        with open("excel files/most-popular.csv") as popular:
            data = csv.DictReader(popular)
            popularInfo = [(i['category'], i['rank'], i['show_title'], i['season_title'],
                            i['hours_viewed_first_28_days']) for i in data]
            cursor.executemany("INSERT INTO rankByCountry (category,rank,show_title,season_title,"
                               "hours_viewed_first_28_days) VALUES "
                               "(?,?,?,?,?);", popularInfo)
            popular.close()
    # TODO import the excel spreadsheet
    # TODO check most recent entry to see where to start in excel file / sort DB by date?
    # TODO crawl IMDB to get genre information
    except sqlite3.DatabaseError as dbErr:
        print(dbErr)
    finally:
        con.commit()
        cursor.close()


def genreCrawl(title):
    x = 0
    # TODO run web crawler on entries without genre information


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
    except sqlite3.DatabaseError as dbErr:
        print("Creating tables")
        try:  # TODO update script with extra fields fetched from spider and regenerate tables
            cursor.executescript("""
                       BEGIN;
                       CREATE TABLE rankByCountry(country_name,country_iso2,week,category,weekly_rank,show_title,"
                       "season_title,cumulative_weeks_in_top_10);
                       CREATE TABLE rankByWeek(week,category,weekly_rank,show_title,season_title,weekly_hours_viewed,cumulative_weeks_in_top_10);
                       CREATE TABLE mostPopular(category,rank,show_title,season_title,hours_viewed_first_28_days);
                       COMMIT;""")
        except sqlite3.DatabaseError as dbErr2:
            print(dbErr2)
    finally:
        cursor.close()


con = sqlite3.connect("netflix.db")


def main():
    initialize()
    cursor = con.cursor()
    results = cursor.execute("SELECT * FROM sqlite_master")
    # stub = open("excel files/all-weeks-countries.csv", "rt")
    stub2 = pandas.read_csv("excel files/all-weeks-countries.csv")
    # print(stub2)
    x = max(2, 4)
    print(x)
    cursor.close()
    if con:
        con.close()


main()
