import time
from firebase_db import db

ahi_targets = db().collection('targets').document('ahi').get().to_dict()

datatypes = ["tweet", "reddit_comment", "reddit_post", "stocktwits_post", "yahoo_finance_comment"]


def calculate_ahi(document):
    ahi = 0
    total_weight = 0
    for dt in datatypes:
        mentions = document.get(dt + "_mentions", 0)
        weight = ahi_targets.get(dt + "_weight", 0)
        benchmark = ahi_targets.get(dt + "_benchmark")
        if not benchmark:
            print(f"no ahi benchmark found for datatype {dt}")
        ahi += weight * mentions / benchmark
        total_weight += weight
    if total_weight != 0:
        return ahi / total_weight
    else:
        return 0


def calculate_sentiment(document):
    total_sentiment = 0
    total_weight = 0
    for dt in datatypes:
        sentiment = document.get(dt + "_sentiment", 0)
        weight = ahi_targets.get(dt + "_weight", 0)
        total_sentiment += weight * sentiment
        total_weight += weight
    if total_weight != 0:
        return total_sentiment / total_weight
    else:
        return 0


def calculate_rhi(ticker):
    ahi_history = db()\
        .collection('tickers')\
        .document(ticker)\
        .collection('history')\
        .document('AHI')\
        .get()\
        .to_dict()["history"]
    day_ago = time.time() - 3600 * 24
    week_ago = time.time() - 3600 * 24 * 7
    last_day = [dp["data"] for dp in ahi_history if dp["timestamp"] > day_ago]
    last_week = [dp["data"] for dp in ahi_history if dp["timestamp"] > week_ago]

    if sum(last_day) == 0 or sum(last_week) == 0:
        return None
    day_average = sum(last_day) / len(last_day)
    week_average = sum(last_week) / len(last_week)
    return day_average / week_average


def calculate_sgp(ticker):
    sentiment_history = db() \
        .collection('tickers') \
        .document(ticker) \
        .collection('history') \
        .document('sentiment') \
        .get() \
        .to_dict()["history"]
    day_ago = time.time() - 3600 * 24
    week_ago = time.time() - 3600 * 24 * 7
    last_day = [dp["data"] for dp in sentiment_history if dp["timestamp"] > day_ago]
    last_week = [dp["data"] for dp in sentiment_history if dp["timestamp"] > week_ago]

    if sum(last_day) == 0 or sum(last_week) == 0:
        return None
    day_average = sum(last_day) / len(last_day)
    week_average = sum(last_week) / len(last_week)
    return day_average / week_average
