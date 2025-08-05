FROM golang:1.24.5-alpine
WORKDIR /app
COPY ./main .
CMD ["./main"]