docker compose -f .\payment-processor\docker-compose.yml up -d
$env:GOOS = "linux"
$env:GOARCH = "amd64"
$env:GOAMD64 = "v3"
go build -ldflags="-s -w" -o main .
docker build --no-cache -t daviddeltasierra/rinha-2025-go-2:1.28 .
docker compose up -d
Start-Sleep -Seconds 5
k6 run /rinha-test/rinha.js
