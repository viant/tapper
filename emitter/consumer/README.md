# Rotation event web consumer

### Introduction

This package defines a rotation event consumer that maps GET request into CLI command.
It is intended as example how to consumer rotation event.

### Configuration

- *port*: endpoint port
- *streams*: collection of data streams matched by URI
  * *URI*: matching URI   
  * *Name*: shall command name
  * *Args*: shall command arguments slice

example:

```yaml
port: 8083
streams:
  - URI: /log/datastream1
    Name: /bin/bash
    Args:
      - /opt/app/script/datastream1.sh
      - $DestPath
      - $DestName
      - $TimePath

  - URI: /log/datastream1
    Name: /bin/bash
    Args:
      - /opt/app/script/datastream1.sh
      - $DestPath
      - $DestName
      - $TimePath
```


where [/opt/app/script/datastream1.sh]() 

```bash
#!/bin/bash

location=$1
filename=$2
timepath=$3

if [ "$location" == "" ]; then
    echo "location was empty"
    exit 1
fi
echo "cp ${location} gs://my.bucket/data/logs/${timepath}/${filename}"
```

## Building service

```
git clone https://github.com/viant/tapper.git
cd tapper/emitter/consumer/app
go build streamer.go
```
