# Docker Notes

* Build
```
docker build -t nomad-events-to-slack .
```

Build with output:
```
docker run -d --name nomad-events-to-slack --env-file .env nomad-events-to-slack
```
