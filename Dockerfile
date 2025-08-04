FROM golang:1.23.0
WORKDIR /app
COPY ./main .
CMD ["./main"]