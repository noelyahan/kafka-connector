# TODO reduve commit overhead, intrduce env var for yaml
FROM golang:latest
ENV GO111MODULE=on
ARG FIRESTORE=github.com/noelyahan/kafka-connect-firestore
ARG CONNECTOR=github.com/gmbyapa/kafka-connector@v0.0.1-beta.0.20190711092410-7c958c4bc155
RUN git clone https://$FIRESTORE $GOPATH/src/$FIRESTORE
WORKDIR $GOPATH/src/$FIRESTORE
RUN go build -buildmode=plugin
COPY ./kafka-connect-firestore.so /opt
WORKDIR $GOPATH/pkg/mod/$CONNECTOR/main
RUN go build -o main_app
COPY main_app ../
COPY ./config.yaml $GOPATH/pkg/mod/$CONNECTOR
WORKDIR $GOPATH/pkg/mod/$CONNECTOR
EXPOSE 8888
CMD ["./main_app"]