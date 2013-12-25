#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__ = "Aravindan Mahendiran"
__email__ = "aravind@vt.edu"
__processor__ = "CountTweets"
__version__ = "1.0.0"

import sys
import os
import json
import traceback
path = "/home/aravindan/Dropbox/git/geocoding/twitter"
if path not in sys.path:
    sys.path.insert(1, path)
from embers.geocode import Geo
from etool import args, logs
from datetime import datetime
log = logs.getLogger(__processor__)

if __name__ == "__main__":
    ap = args.get_parser()
    ap.add_argument('-t', '--tweetFolder', type=str,
                    help='inputFolder pointing to PSLs output',
                    default='/hdd/tweets/2012/oct')
    ap.add_argument('-c', '--country', type=str)
    ap.add_argument('-m', '--month', type=str)
    arg = ap.parse_args()
    logs.init(arg)
    geo = Geo()
    tweetCount = 0
    date = datetime.strptime(arg.month, "%b %Y")
    for _file in os.listdir(arg.tweetFolder):
        try:
            with open(arg.tweetFolder + "/" + _file, "r") as FILE:
                for line in FILE:
                    try:
                        jsonTweet = json.loads(line.strip())
                        dateStr = jsonTweet['interaction']['created_at'][5:16]
                        tweetDate = datetime.strptime(dateStr, '%d %b %Y')
                        geoList = geo.geo_normalize(jsonTweet)
                        city, ctry, state = geoList[:3]
                        if ctry and ctry.lower() == arg.country.lower() and date.month == tweetDate.month and date.year == tweetDate.year:
                            tweetCount += 1
                    except Exception, f:
                        _traceback = sys.exc_info()[2]
                        log.exception("error processing tweet %s @line %s" % (f, traceback.tb_lineno(_traceback)))
                log.info("tweet Count -->" + str(tweetCount))
        except:
            log.exception('error opening file')

    print ('# tweets for %s in %s --> %d' % (arg.country, arg.month, tweetCount))
