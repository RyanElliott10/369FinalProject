import sys
import pandas as pd
from datetime import datetime
import requests
import os



def parseTime(inputdata):
    timestamps = []

    # two to account for open/close
    timestamps.extend(inputdata["chart"]["result"][0]["timestamp"])
    timestamps.extend(inputdata["chart"]["result"][0]["timestamp"])

    calendertime = []

    for ts in timestamps:
        dt = datetime.fromtimestamp(ts)
        calendertime.append(dt.strftime("%m/%d/%Y"))

    return calendertime


def parseValues(inputdata):
    values = []
    values.extend(inputdata["chart"]["result"][0]["indicators"]["quote"][0]["open"])
    values.extend(inputdata["chart"]["result"][0]["indicators"]["quote"][0]["close"])

    return values


# add market open/close attribute
def attachEvents(inputdata):

  eventlist = []

  for i in range(0,len(inputdata["chart"]["result"][0]["timestamp"])):
    eventlist.append("open")

  for i in range(0,len(inputdata["chart"]["result"][0]["timestamp"])):
    eventlist.append("close")

  return eventlist



# usage = $ python2 fetch.py <interval> <symbol> <range> <region> <time>
#  interval values = {1m, 2m, 5m, 15m, 60m, 1d}
#  symbol values = { company's ticker }
#  range values = { 1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max}
#  region values = {US, BR, AU, etc...}

def main():

    # account specific values
    RAPIDAPI_KEY = "883de80fefmsh5ce969c2da68157p16b45djsndddadb019c2c"
    RAPIDAPI_HOST = "apidojo-yahoo-finance-v1.p.rapidapi.com"

    url = "https://apidojo-yahoo-finance-v1.p.rapidapi.com/stock/v2/get-chart"

    interval = sys.argv[1]
    symbol = sys.argv[2].upper()
    range = sys.argv[3]
    region = sys.argv[4].upper()

    querystring = {"interval": interval, "symbol": symbol, "range": range, "region": region}
    headers = {'x-rapidapi-key': RAPIDAPI_KEY, 'x-rapidapi-host': RAPIDAPI_HOST}


    response = (requests.request("GET", url, headers=headers, params=querystring)).json()


    data = pd.DataFrame()

    if (response != None):
        data["TimeStamp"] = parseTime(response)
        data["Values"] = parseValues(response)
        data["Events"] = attachEvents(response)


    # write to specified output directory
    path = os.getcwd() + "/stockData/output.csv"
    data.to_csv(path)

    # write complementary descriptor file
    descriptors = [symbol, range]
    path2 = os.getcwd() + "/stockData/descriptor.csv"
    file = open(path2, 'w')
    file.write(",".join(descriptors))
    file.close()




if __name__== '__main__':
    main()

