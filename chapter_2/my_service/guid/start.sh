#!/bin/sh

HOST=`echo $1 | cut -d ":" -f 1`
PORT=`echo $1 | cut -d ":" -f 2`
export CONFIG_PATH=app.ini
export APP_ENDPOINT=$1

uvicorn main:app --reload --host $HOST --port $PORT 
