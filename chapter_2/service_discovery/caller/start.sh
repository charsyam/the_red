#!/bin/sh

HOST=`echo $1 | cut -d ":" -f 1`
PORT=`echo $1 | cut -d ":" -f 2`
APP_ENDPOINT=$1 uvicorn main:app --reload --host $HOST --port $PORT
