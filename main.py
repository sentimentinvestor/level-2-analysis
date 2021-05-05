import time
from flask import jsonify
from firebase_db import db
from firebase_admin import firestore
from analysis_tools import calculate_ahi, calculate_sentiment, calculate_rhi, calculate_sgp
import base64


# triggered by a pubsub message to the 'level_2_analysis' topic
# uploads the number of mentions and the sentiment for a particular ticker to
# that ticker's document
def level_2_analysis(event, context):
    start_time = time.time()
    ticker = base64.b64decode(event['data']).decode('utf-8')
    updated_fields = {}
    document = db().collection('tickers').document(ticker).get().to_dict()

    updated_fields["previous_AHI"] = document["AHI"]
    updated_fields["previous_sentiment"] = document["sentiment"]

    ahi = calculate_ahi(document)
    sentiment = calculate_sentiment(document)
    updated_fields["AHI"] = ahi
    updated_fields["AHI_timestamp"] = time.time()
    updated_fields["sentiment"] = sentiment
    updated_fields["sentiment_timestamp"] = time.time()
    db().collection('tickers')\
        .document(ticker)\
        .collection('history')\
        .document('AHI').set({
        "history": firestore.ArrayUnion([{
            "timestamp": time.time(),
            "data": ahi
        }])
    }, merge=True)
    db().collection('tickers') \
        .document(ticker) \
        .collection('history') \
        .document('sentiment').set({
        "history": firestore.ArrayUnion([{
            "timestamp": time.time(),
            "data": sentiment
        }])
    }, merge=True)

    # these two functions must be run after the upload of AHI and sentiment
    rhi = calculate_rhi(ticker)
    sgp = calculate_sgp(ticker)
    updated_fields["RHI"] = rhi
    updated_fields["RHI_timestamp"] = time.time()
    updated_fields["SGP"] = sgp
    updated_fields["SGP_timestamp"] = time.time()
    db().collection('tickers') \
        .document(ticker) \
        .collection('history') \
        .document('RHI').set({
        "history": firestore.ArrayUnion([{
            "timestamp": time.time(),
            "data": rhi
        }])
    }, merge=True)
    db().collection('tickers') \
        .document(ticker) \
        .collection('history') \
        .document('SGP').set({
        "history": firestore.ArrayUnion([{
            "timestamp": time.time(),
            "data": sgp
        }])
    }, merge=True)

    db().collection('tickers').document(ticker).set(updated_fields, merge=True)

    return jsonify({
        "success": True,
        "time_taken": time.time() - start_time
    })
