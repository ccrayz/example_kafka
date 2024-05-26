
# Run Docker Compose

```
# run kafka ans opensearch
docker compse up -d
```

### Setup env
```
cp .env.copy .env
```


### Run Producer
```
# new terminal
go run main.go -p

```

### Run Consumer
```
# new terminal
go run main.go -c

```