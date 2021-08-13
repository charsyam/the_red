FROM python:3.8
LABEL maintainer="charsyam@naver.com"

COPY . /app/the_red/scrap
WORKDIR /app/the_red/scrap

RUN pip3 install -r ./requirements.txt
ENV ENDPOINTS=127.0.0.1:8001

# container가 구동되면 실행
ENTRYPOINT ./start.sh "${ENDPOINTS}"
