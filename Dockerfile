FROM scratch

ARG MONGO_PASS

ENV MONGO_HOST=twitter-votes-mongodb MONGO_PORT=27017 MONGO_DB=ballots MONGO_USER=mongo MONGO_PASS=${MONGO_PASS} MONGO_SOURCE=ballots
ENV NSQ_HOST=twitter-votes-nsqlookupd NSQ_PORT=4161 NSQ_TOPIC=votes

COPY counter .

ENTRYPOINT ["./counter"]