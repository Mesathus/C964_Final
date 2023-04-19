import os
from datetime import datetime as dt
from datetime import date


def log(error) -> None:
    try:
        basePath = os.getcwd()
        errDate = date.today()
        errTime = dt.utcnow()
        errPath = os.path.join(basePath, 'logs')
        if not os.path.exists(errPath):  # make the logs directory if it doesn't exist
            os.makedirs(errPath)
        errPath = os.path.join(errPath, str(errDate) + '.txt')  # append the log file name to the logs directory path
        with open(errPath, 'a') as errFile:
            errFile.write(f"{errTime} : The following error occurred\n {error}\n")
            errFile.close()
    except OSError as oErr:
        print("An error has occurred while logging.")
        print(oErr)


def createManualUpdateList(showList):
    try:
        basePath = os.getcwd()
        errDate = date.today()
        errTime = dt.utcnow()
        errPath = os.path.join(basePath, 'Manual Updates')
        if not os.path.exists(errPath):  # make the manual updates directory if it doesn't exist
            os.makedirs(errPath)
        errPath = os.path.join(errPath, str(errDate) + '.txt')  # append the log file name to the logs directory path
        with open(errPath, 'a') as errFile:
            errFile.write(f"{errDate} : The following shows need manual genre updates: \n {(lambda x: chr(13).join(x))(showList)}")
            errFile.close()
    except OSError as oErr:
        print("An error has occurred while logging.")
        print(oErr)
