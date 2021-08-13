FROM python:3.8
LABEL maintainer="charsyam@naver.com"

COPY . /app/the_red/guid
WORKDIR /app/the_red/guid

RUN pip3 install -r ./requirements.txt
ENV ENDPOINTS=127.0.0.1:7001
ENV WORKER_ID=0
ENV DATACENTER_ID=0

# container가 구동되면 실행
ENTRYPOINT ./start.sh "${ENDPOINTS}"
