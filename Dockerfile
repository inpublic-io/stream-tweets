FROM golang:1.17-alpine as build

ADD . /go/src/github.com/inpublic-io/stream-tweets

WORKDIR /go/src/github.com/inpublic-io/stream-tweets

RUN go build -o "service" -tags musl ./

FROM alpine:3

LABEL org.opencontainers.image.source https://github.com/inpublic-io/stream-tweets

RUN apk update \
	&& apk -U upgrade \
	&& apk add --no-cache ca-certificates bash \
	&& update-ca-certificates --fresh \
	&& rm -rf /var/cache/apk/*

# adds inpublic user
RUN	addgroup inpublic \
	&& adduser -S inpublic -u 1000 -G inpublic

USER inpublic

COPY --from=build --chown=inpublic:inpublic /go/src/github.com/inpublic-io/stream-tweets/service /usr/local/bin/
RUN chmod +x /usr/local/bin/service

ENTRYPOINT [ "/usr/local/bin/service" ]