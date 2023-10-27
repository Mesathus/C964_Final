import numpy
import pandas as pd
from imdb import Cinemagoer
import matplotlib as mpl
from sklearn.metrics import classification_report, accuracy_score, confusion_matrix
import Logger
import tkinter as tk
from tkinter import *
from tkinter import ttk
from sklearn import linear_model
from sklearn.model_selection import train_test_split
from sklearn import preprocessing
import scikitplot as skplot
import seaborn as sns
import sys
import csv
import sqlite3


def updateDatabase():
    cursor = con.cursor()
    try:
        # generate the first three databases from csv files
        with open("excel files/all-weeks-countries.csv", "rt", encoding='utf-8') as allCountries:
            data = csv.DictReader(allCountries)
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
        with open("excel files/genre-info.csv", "rt", encoding='utf-8') as genres:
            data = csv.DictReader(genres)
            genreInfo = [(i['title'], i['genres']) for i in data]
            cursor.executemany("INSERT OR IGNORE INTO genreInfo (title, genres) VALUES "
                               "(?,?);", genreInfo)
            genres.close()
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
        manualUpdates()
    except sqlite3.DatabaseError as dbErr:
        print(dbErr)
        con.rollback()
    except Exception as ex:
        print(ex)
        print(ex.__traceback__)
    finally:
        con.commit()
        cursor.close()


def genreCrawl(title, year, season) -> str:  # TODO make this function better
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


class BuildGUI:
    def __init__(self, connection, root):
        self.cursor = connection.cursor()
        self.root = root
        # declare GUI elements
        # frames
        self.chkFrame = ttk.Frame(self.root)
        self.graphFrame = ttk.Frame(self.root)
        self.framePredict = ttk.Frame(self.root)
        self.frameCompare = ttk.Frame(self.root)
        # variables
        self.movies = tk.BooleanVar()
        self.tvShows = tk.BooleanVar()
        self.country = tk.StringVar()
        self.genre = tk.StringVar()
        # interactables
        self.countryCBox = ttk.Combobox(self.root, width=50, textvariable=self.country)
        self.genreCBox = ttk.Combobox(self.root, width=50, textvariable=self.genre)
        self.btnStuff = ttk.Button(self.root, command=self.recommender, width=30, text="Predict")
        self.exitMenu = Menu(self.root)
        self.root.config(menu=self.exitMenu)
        self.chkMovie = ttk.Checkbutton(master=self.chkFrame, text="Movies", variable=self.movies, onvalue=True,
                                        offvalue=False)
        self.chkTV = ttk.Checkbutton(master=self.chkFrame, text="TV Series", variable=self.tvShows, onvalue=True,
                                     offvalue=False)
        self.recommendLabel = ttk.Label(self.root)
        self.imdbGUI()

    def recommender(self):
        try:
            print(self.country.get(), self.genre.get(), self.movies.get(), self.tvShows.get())
            category = " IS NULL " if not self.tvShows.get() and not self.movies.get() else " LIKE '%' " \
                if self.tvShows.get() and self.movies.get() else " = 'TV' " if self.tvShows.get() else " = 'Films' "
            # query to match genre/country/category by all years
            df2021rank = pd.DataFrame(self.cursor.execute(f"SELECT show_title,season_title,MAX(cumulative_weeks_in_top_10),week,genres "
                                                          f"FROM rankByCountry "
                                                          f"LEFT JOIN genreInfo ON rankByCountry.show_title = genreInfo.title "
                                                          f"WHERE country_name = '{self.country.get()}' "
                                                          f"AND category {category} "
                                                          f"GROUP BY show_title"))
            cumWeeks = df2021rank[2].apply(lambda key: int(key)).to_numpy()  # get an array of cumulative weeks column for later
            # transform genres to binary to match search criteria
            df2021rank[4] = df2021rank[4].apply(lambda key: 1 if self.genre.get().lower() in key else 0)  # 4 = genre column
            df2021rank[2] = df2021rank[2].apply(lambda key: 1 if int(key) > 1 else 0)  # 2 = MAX aggregate column
            model = linear_model.LogisticRegression()
            x = df2021rank[4].to_numpy().reshape(-1, 1)
            y = df2021rank[2].to_numpy()
            model.fit(x, y)
            r_sq = model.score(x, y)
            X_train, X_test, y_train, y_test = train_test_split(x, y, test_size=0.20, random_state=5, stratify=y)
            scaler = preprocessing.StandardScaler().fit(X_train)
            X_train_scaled = scaler.transform(X_train)
            model.fit(X_train_scaled, y_train)
            train_acc = model.score(X_train_scaled, y_train)
            print(f"slope: {model.coef_}")
            print(f"intercept: {model.intercept_}")
            print(f"coefficient of determination: {r_sq}")
            print("The Accuracy for Training Set is {}%".format(round(train_acc * 100, 3)))
            X_test_scaled = scaler.transform(X_test)
            y_pred = model.predict(X_test_scaled)
            test_acc = accuracy_score(y_test, y_pred)
            print("The Accuracy for Test Set is {}%".format(round(test_acc * 100, 3)))
            print(classification_report(y_test, y_pred))
            # setting up confusion matrix
            cm = confusion_matrix(y_test, y_pred)
            mpl.pyplot.figure(figsize=(12, 6))
            mpl.pyplot.title("Confusion Matrix")
            sns.heatmap(cm, annot=True, fmt='d', cmap='Blues')
            mpl.pyplot.ylabel("Actual Values")
            mpl.pyplot.xlabel("Predicted Values")
            mpl.pyplot.savefig('confusion_matrix.png')
            mpl.pyplot.show()
            # setting up ROC
            y_pred = model.predict_proba(X_test_scaled)
            skplot.metrics.plot_roc(y_test, y_pred)
            mpl.pyplot.savefig('roc.png')
            mpl.pyplot.show()
            # setting up histogram
            fig, ax = mpl.pyplot.subplots()
            ax.hist(cumWeeks, bins=cumWeeks.max(), linewidth=0.5, edgecolor="white")
            ax.set(xlim=(0, cumWeeks.max()), xticks=numpy.arange(1, cumWeeks.max()),
                   ylim=(0, len(cumWeeks)), yticks=numpy.linspace(0, len(cumWeeks), 9))
            mpl.pyplot.ylabel("Instances of weekly ranking")
            mpl.pyplot.xlabel("Consecutive weeks in top 10")
            mpl.pyplot.savefig('histogram.png')
            mpl.pyplot.show()
            if test_acc > .7:
                self.recommendLabel.configure(text=f"With a test accuracy of {round(test_acc * 100, 2)}%, there's a strong "
                                                   f"correlation \nand this project is recommended.")
            elif test_acc > .5:
                self.recommendLabel.configure(text=f"With a test accuracy of {round(test_acc * 100, 2)}%, there's a weak "
                                                   f"correlation.  \nThis project is recommended after more regional research.")
            else:
                self.recommendLabel.configure(text=f"With a test accuracy of {round(test_acc * 100, 2)}%, there doesn't "
                                                   f"appear to be much correlation.  \nThis project isn't recommended.")
        except TypeError as TErr:
            print(TErr)
        except sqlite3.DatabaseError as DBErr:
            print(DBErr)

    def imdbGUI(self):
        try:
            self.root.geometry('500x500')
            df = pd.DataFrame(self.cursor.execute("SELECT DISTINCT country_name FROM rankByCountry;"))
            # labels
            regionLabel = ttk.Label(self.root, text="Select a country: ")
            genreLabel = ttk.Label(self.root, text="Select a genre: ")
            # assign additional values to GUI
            vals = list(df.to_numpy(dtype=str).flatten())  # to_numpy gives a tuple, flatten before we convert to a list
            self.countryCBox['values'] = vals  # if we don't convert to a list combobox breaks words with spaces to separate rows
            df = pd.DataFrame(self.cursor.execute("SELECT * FROM genreInfo;"))
            vals = list(df[1].to_numpy(dtype=str).flatten())
            genreSet = set()  # create a set of genres, so we don't add duplicates to the combobox
            # remove list artifacts from genre strings before adding them to the set
            for x in vals:
                s = x.replace('[', '').replace(']', '').replace('\'', '').split(',')
                for y in s:
                    if y.lower().strip() != 'none':
                        genreSet.add(y.strip().capitalize())  # formatting the items in the genre set
            self.genreCBox['values'] = sorted(list(genreSet))  # sets can't be sorted, so convert back to a list
            self.exitMenu.add_command(label="Exit", command=self.close)
            # add elements to the root window
            self.countryCBox.pack()
            self.genreCBox.pack()
            self.btnStuff.pack()
            self.chkFrame.pack()
            self.chkMovie.pack()
            self.chkTV.pack()
            self.recommendLabel.pack()
        except TypeError as TErr:
            print(TErr)
        except sqlite3.DatabaseError as DBErr:
            print(DBErr)

    def close(self):
        exit()
        self.root.destroy()  # close the tkinter window without exiting the program

    def __del__(self):
        try:
            self.cursor.close()
        except sqlite3.ProgrammingError:
            return


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


def manualUpdates():
    cursor = con.cursor()
    try:
        df = pd.DataFrame(cursor.execute("SELECT * FROM genreInfo WHERE genres = 'none' ORDER BY title"))
        if df.values.size > 0:
            Logger.createManualUpdateList(df)
    except sqlite3.DatabaseError as DErr:
        Logger.log(DErr)
    finally:
        cursor.close()


def initializeDatabase() -> None:
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
                       CREATE TABLE IF NOT EXISTS genreInfo(title, genres,
                       UNIQUE (title, genres));
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
    # uncomment the next to lines to rebuild databases from scratch
    # resetDatabase()
    # initializeDatabase()
    cursor = con.cursor()
    root = Tk()
    print("Updating tables")
    updateDatabase()
    app = BuildGUI(con, root)
    root.mainloop()
    cursor.close()
    if con:
        con.close()


con = sqlite3.connect("netflix.db")
cg = Cinemagoer()

if __name__ == "__main__":
    main()
