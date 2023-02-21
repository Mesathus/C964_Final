import re
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from w3lib.url import url_query_cleaner
import extruct
import numpy
import pandas
import mysql.connector
import itertools
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
    x = 0

    # TODO import the excel spreadsheet
    # TODO check most recent entry to see where to start in excel file / sort DB by date?
    # TODO crawl IMDB to get genre information


def genreCrawl(title):
    x = 0
    # TODO run web crawler on entries without genre information


def recommender():
    x = 0
    # TODO collect user input
    # TODO search DB for matching information and run regression
    # TODO make recommendations based on criteria (user specified?)


def initialize():
    try:
        cursor.execute("SELECT * FROM rankByCountry")
    except sqlite3.DatabaseError as dbErr:
        print("Creating tables")
        try:
            cursor.executescript("""
                       BEGIN;
                       CREATE TABLE rankByCountry(country_name,country_iso2,week,category,weekly_rank,show_title,"
                       "season_title,cumulative_weeks_in_top_10);
                       CREATE TABLE rankByWeek(week,category,weekly_rank,show_title,season_title,weekly_hours_viewed,cumulative_weeks_in_top_10);
                       CREATE TABLE mostPopular(category,rank,show_title,season_title,hours_viewed_first_28_days);
                       COMMIT;""")
        except sqlite3.DatabaseError as dbErr2:
            print(dbErr2)


con = sqlite3.connect("netflix.db")
cursor = con.cursor()


def main():
    initialize()
    results = cursor.execute("SELECT * FROM sqlite_master")
    # stub = open("excel files/all-weeks-countries.csv", "rt")
    stub2 = pandas.read_csv("excel files/all-weeks-countries.csv")
    # print(stub2)
    x = max(2, 4)
    print(x)


main()
