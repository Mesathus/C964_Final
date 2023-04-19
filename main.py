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
import Logger
import tkinter as tk
from tkinter import *
from tkinter import ttk
import sys
import csv
import sqlite3


def updateDatabase():
    cursor = con.cursor()
    try:
        # generate the first three databases from csv files
        # TODO write a function to handle database creation
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
        if df.values.size > 0:  # check if genre table even needs to be updated    change back to if > 0 when done
            earliestYear = int(pd.DataFrame(cursor.execute("SELECT week FROM rankByCountry WHERE season_title != '' "
                                                           "ORDER BY week ASC LIMIT 1"))[0].values[0].split('-')[0])
            latestYear = int(pd.DataFrame(cursor.execute("SELECT week FROM rankByCountry WHERE season_title != '' "
                                                         "ORDER BY week DESC LIMIT 1"))[0].values[0].split('-')[0])
            while earliestYear <= latestYear:
                # update movies
                df = pd.DataFrame(cursor.execute(f"SELECT DISTINCT show_title FROM "
                                                 f"(SELECT * FROM rankByCountry LEFT JOIN genreInfo ON show_title = title "
                                                 f"WHERE genres IS NULL) WHERE week LIKE '{earliestYear}%' AND season_title = '' LIMIT 200"))
                if df.values.size > 0:
                    df = df.rename(columns={0: 'title'})
                    df['genres'] = df.apply((lambda key: genreCrawl(key.title, earliestYear, False)), axis=1)
                    df.to_sql('genreInfo', con, if_exists='append', index=False)
                # update tv shows
                df = pd.DataFrame(cursor.execute(f"SELECT DISTINCT show_title FROM "
                                                 f"(SELECT * FROM rankByCountry LEFT JOIN genreInfo ON show_title = title "
                                                 f"WHERE genres IS NULL) WHERE week LIKE '{earliestYear}%' AND season_title != '' LIMIT 100"))
                if df.values.size > 0:
                    df = df.rename(columns={0: 'title'})
                    df['genres'] = df.apply((lambda key: genreCrawl(key.title, earliestYear, True)), axis=1)
                    df.to_sql('genreInfo', con, if_exists='append', index=False)
                earliestYear += 1
        # print(pd.DataFrame(cursor.execute("SELECT * FROM genreInfo")))
        # df.drop(columns={'week', "season"}, inplace=True)
        # print(pd.DataFrame(cursor.execute("SELECT * FROM genreInfo")))
        args = ['country_name', 'week']  # for creating variable search queries from user selected categories,  not for this module
        query = f"SELECT {(lambda x: ', '.join(x))(args)} FROM rankByCountry ORDER BY show_title DESC LIMIT 50"
        # df = pd.DataFrame(cursor.execute(query))
        # print("\n", df.info)
        # df.rename(mapper=lambda x: args[x], axis=1, inplace=True)  # using the args list to rename columns
        # entry1 = df.at[0, 'week'].split('-')
        # entry2 = df.at[3, 'week'].split('-')
        # print(entry1, "\n", entry2)
    # TODO check most recent entry to see where to start in excel file / sort DB by date?
    except sqlite3.DatabaseError as dbErr:
        print(dbErr)
        con.rollback()
    except Exception as ex:
        print(ex)
        print(ex.__traceback__)
    finally:
        con.commit()
        cursor.close()


def genreCrawl(title, year, season) -> str:  # TODO make this function smarter than selecting the first result
    movie = None
    try:
        movieList = cg.search_movie(title.lower())
        # movieList = list(filter(lambda x: len(x.data['title']) == len(title), movieList))
        closestYear = sys.maxsize
        kind = 'tv series' if season else 'movie'  # python ternary equivalent
        mList = []  # cut part from if statements
        for m in movieList:
            mList.append(cg.get_movie(m.getID()))
        movie = mList[0]  # default to the first/most relevant list item
        mList = list(filter(lambda x: len(x.data['title']) == len(title) and x.data['kind'] == kind, mList))  # end cut
        movie = mList[0]  # adjust default after filtering
        mList = list(filter(lambda x: x.data['title'].lower() == title.lower() and x.data['kind'] == kind, mList))
        movie = mList[0]
        if season:
            for m in mList:
                if year - m.data['year'] < 0:
                    continue  # if m aired after our current update year skip it
                seasons = len(m.data['seasons'])
                if year - m.data['year'] - seasons <= 0:  # check if title could have been airing during current year
                    # update to best fit
                    if m.data['year'] < movie.data['year']:  # bias to the oldest show still running
                        movie = m
                    elif m.data['year'] == movie.data['year']:  # shows debuting the same year
                        if len(m.data['seasons']) > len(movie.data['seasons']):  # bias to shows with more seasons if aired same year
                            movie = m
        else:
            for m in mList:
                if closestYear > (year - m['year']) >= 0:  # bias to most relevant release, make sure closest isn't negative
                    movie = m
                    closestYear = year - m['year']
    except KeyError as kErr:
        print(kErr)
        print(title)
        print(movie)
        Logger.log(kErr)
        Logger.log(movieList)
    except TypeError as tErr:
        print(tErr)
        print(title)
        print(movie)
        Logger.log(tErr)
        Logger.log(movieList)
    except Exception as ex:
        print(ex)
        print(title)
        print(movie)
        Logger.log(ex)
        Logger.log(movieList)
    finally:
        #  return in the finally clause, so we can use mList[0] if an
        #  Error is thrown from one of IMDBs weird classifications
        return getGenres(movie)


class BuildGUI:  # this would be better as a class
    # TODO build GUI
    def __init__(self, connection, root):
        self.cursor = connection.cursor()
        try:
            root.geometry('500x500')
            df = pd.DataFrame(self.cursor.execute("SELECT DISTINCT country_name FROM rankByCountry;"))
            # declare GUI elements
            # frames
            self.chkFrame = ttk.Frame(root)
            self.graphFrame = ttk.Frame(root)
            # variables
            self.movies = tk.BooleanVar()
            self.tvShows = tk.BooleanVar()
            self.country = tk.StringVar()
            self.genre = tk.StringVar()
            # interactables
            self.countryCBox = ttk.Combobox(root, width=50, textvariable=self.country)
            self.genreCBox = ttk.Combobox(root, width=50, textvariable=self.genre)
            self.btnStuff = ttk.Button(root, command=self.recommender, width=30, text="Predict")
            self.exitMenu = Menu(root)
            root.config(menu=self.exitMenu)
            self.chkMovie = ttk.Checkbutton(master=self.chkFrame, text="Movies", variable=self.movies, onvalue=True, offvalue=False)
            self.chkTV = ttk.Checkbutton(master=self.chkFrame, text="TV Series", variable=self.tvShows, onvalue=True, offvalue=False)
            # labels
            regionLabel = ttk.Label(root, text="Select a country: ")
            genreLabel = ttk.Label(root, text="Select a genre: ")
            # assign additional values to GUI
            self.chkMovie.selection_clear()
            vals = list(df.to_numpy(dtype=str).flatten())  # to_numpy gives a tuple, flatten before we convert to a list
            self.countryCBox['values'] = vals  # if we don't convert to a list combobox breaks words with spaces to separate rows
            df = pd.DataFrame(self.cursor.execute("SELECT genres FROM genreInfo;"))
            vals = list(df.to_numpy(dtype=str).flatten())
            genreSet = set()
            for x in vals:
                s = x.replace('[', '').replace(']', '').replace('\'', '').split(',')
                for y in s:
                    genreSet.add(y.strip().capitalize())
            # genreSet = set(itertools.chain(vals))  # test this https://datagy.io/python-flatten-list-of-lists/
            self.genreCBox['values'] = list(genreSet)
            self.framePredict = ttk.Frame()
            self.frameCompare = ttk.Frame()
            self.exitMenu.add_command(label="Exit", command=exit)
            # add elements to the root window
            self.countryCBox.pack()
            self.genreCBox.pack()
            self.btnStuff.pack()
            self.chkFrame.pack()
            self.chkMovie.pack()
            self.chkTV.pack()
        except TypeError as TErr:
            print(TErr)
        except sqlite3.DatabaseError as DBErr:
            print(DBErr)

    def recommender(self):
        cursor = con.cursor()
        # TODO collect user input
        # TODO search DB for matching information and run regression
        # TODO make recommendations based on criteria (user specified?)
        try:
            print(self.country.get(), self.genre.get(), self.movies.get(), self.tvShows.get())
        except TypeError as TErr:
            print(TErr)
        except sqlite3.DatabaseError as DBErr:
            print(DBErr)
        finally:
            cursor.close()

    def __del__(self):
        if self.cursor:
            self.cursor.close()


def filterDate(csvFile):  # TODO compare existing database most recent date to csv file dates
    cursor = con.cursor()


def getGenres(movie) -> str:
    genres = []
    try:
        genre = movie.get('genres')
        for g in genre:
            genres.append(g.lower())
    except TypeError as TErr:
        Logger.log(TErr)
        Logger.log(movie)
        return "none"
    except AttributeError as AErr:
        Logger.log(AErr)
        Logger.log(movie)
        return "none"
    except Exception as ex:
        print(ex)
    return str(genres)


def recommender(country, genres, movie, tv):
    cursor = con.cursor()
    # TODO collect user input
    # TODO search DB for matching information and run regression
    # TODO make recommendations based on criteria (user specified?)
    try:
        print(country, genres, movie, tv)
    except TypeError as TErr:
        print(TErr)
    except sqlite3.DatabaseError as DBErr:
        print(DBErr)
    finally:
        cursor.close()


def initialize() -> None:
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


def resetDatabase() -> None:
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


def main():
    # resetDatabase()
    # initialize()
    cursor = con.cursor()
    root = Tk()
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
    print("Updating tables")
    updateDatabase()
    df5 = pd.DataFrame(cursor.execute("SELECT DISTINCT(show_title), season_title, genres FROM rankByCountry "
                                      "LEFT JOIN genreInfo ON rankByCountry.show_title = genreInfo.title "
                                      "WHERE genreInfo.genres IS NULL ORDER BY show_title"))

    app = BuildGUI(con, root)
    root.mainloop()
    args = ['title', 'genres']  # for creating variable search queries from user selected categories,  not for this module
    query = f"SELECT {(lambda x: ', '.join(x))(args)} FROM genreInfo ORDER BY title"
    df = pd.DataFrame(cursor.execute(query))
    df.rename(mapper=lambda x: args[x], axis=1, inplace=True)  # using the args list to rename columns
    # rename the columns for the csv
    # genreCSV = df.to_csv('excel files/genre-info.csv', index=False)
    df = pd.DataFrame(cursor.execute("SELECT * FROM genreInfo WHERE genres = 'none' ORDER BY title"))


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


sys.stdout.reconfigure(encoding='utf-8')
con = sqlite3.connect("netflix.db")
cg = Cinemagoer()
if __name__ == "__main__":
    main()
