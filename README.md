# High performant transaction logger for go

[![Application operation performance metric.](https://goreportcard.com/badge/github.com/viant/tapper)](https://goreportcard.com/report/github.com/viant/tapper)
[![GoDoc](https://godoc.org/github.com/viant/tapper?status.svg)](https://pkg.go.dev/mod/github.com/viant/tapper)

This library is compatible with Go 1.12+

Please refer to [`CHANGELOG.md`](CHANGELOG.md) if you encounter breaking changes.

- [Introduction](#motivation)
- [Usage](#usage)
- [Configuration](#configuration)
- [Messages](#messages)
- [License](#license)
- [Credits and Acknowledgements](#credits-and-acknowledgements)

### Introduction

The goal of this project is to provide hyper performant, zero memory allocation transaction logger,
that can work with local and cloud file storage.
Tapper logger allow log rotation, where each rotation can be optionally post rotation logic can be delegated to external web service
or a shell script.


### Usage

```go

cfg := &config.Stream{
    URL: "/tmp/logfile.log",
    Rotation: &config.Rotation{
        EveryMs: 20000,
        URL:     "s3://my.bucket/data/logfile.log.[yyyyMMdd_HH]-%v",
    },
}
logger, err := log.New(cfg, "myID", afs.New())
if err != nil {
    slog.Fatal(err)
}
provider := msg.NewProvider(2048, 32)
for i :=0;i<100;i++ {
    message := provider.NewMessage()
    message.PutString("k1", "value1")
    message.PutInt("k2", 2)
    message.PutStrings("k3", []string{"1", "3"})
    err = logger.Log(message)
    if err != nil {
        slog.Fatal(err)
    }
    message.Free()
}
logger.Close()

```

### Configuration

- **URL**:  location of main log stream
- **FlushMod**: optional flush frequency (testing only, do not use on production)
- **Codec**: optional compression codec (gzip) of main stream not recommended for production, compressing on rotation is much faster) 

- **Rotation**: optional rotation config where:
    - **EveryMs**: rotation frequency in ms
    - **Codec**:  optional compression codec (gzip) applied on log rotation.
    - **URL**: rotation dest pattern
    - **Emit**: optional rotation event notification vi URL or OS process (shell command) 
        * **URL** URL to call with specified parameters
        * **Params** URL parameters (query string)
        * **Name**: name of command to run
        * **Args**: command arguments
    - in Args or Params values you can use the following variables:
        * $DestPath variable to refer to rotated absolute file name  
        * $Dest to expand with dest URL 
        * $DestName to expand with simple file name 
        * $TimePath yyyy/mm/dd/hh rotation create time base path fragment

##### Rotation URL pattern expression
    - time expression placed in squere brackets: [yyyy-MM-dd_HH]
    - logger ID - rotation seqence: %v

##### Configuring rotation event with 3rd party web service

The following configuration drives rotation notification on http://127.0.0.1:8083 
vi REST service as GET request.
```yaml

URL: /opt/app/logs/datastream1.log
FlushMod: 1
Rotation:
  EveryMs: 30000
  URL: /opt/app/logs/datastream1.log.[yyyy-MM-dd_hh-mm-ss].%v
  Emit:
    URL: http://127.0.0.1:8083/log/datastream1
    Params:
      DestPath: $DestPath
      DestName: $DestName
      TimePath: $TimePath
```

See [event consumer](emitter/consumer) service example.


##### Configuring rotation event with a shell script

```yaml
URL: /opt/app/logs/datastream1.log
FlushMod:
Rotation:
  EveryMs: 10000
  URL: /opt/app/logs/datastream1.log.[yyyy-MM-dd_hh-mm-ss].%v
  Emit:
    Command: /bin/bash
    Args:
      - /opt/mediator/script/sitelet.sh
      - $DestPath
      - $DestName
      - $TimePath
```

### Messages

To reduce log message memory overhead, a message can be created by [Provider](msg/provider.go), which 
handles data pooling. You can create a log message with the following snippet.

```go
provider := msg.NewProvider(avgMessageSize, concurrency)
message := provider.NewMessage()
defer message.Free()
```
One message is no longer needed Free method returns it back to the provider pool.


Log message [support](io/stream.go) primitive and complex data structure.

```go
meesage.Put([]byte{`"k1":"raw data"`})
meesage.PutString("k2", "v2")
meesage.PutNonEmptyString("k2.1", v)
meesage.PutB64EncodedBytes("k2.2", rawData)
meesage.PutInt("k3", 3)
meesage.PutFloat("k3", 3.2)
meesage.PutBool("k3", false)
meesage.PutInts("k4", []int{1,2,3})
meesage.PutObject("k5", object)
meesage.PutObjects("k6", objects)
```

### Benchmark

Benchmark builds b.T x 1K message with 10 attrs and writes the log stream.

```bash
BenchmarkLogger_Log
BenchmarkLogger_Log-16             	  258068	      4348 ns/op	       0 B/op	       0 allocs/op
BenchmarkLogger_Log_Rotation
BenchmarkLogger_Log_Rotation-16    	  211363	      5318 ns/op	       5 B/op	       0 allocs/op
```


## License

The source code is made available under the terms of the Apache License, Version 2, as stated in the file `LICENSE`.

Individual files may be made available under their own specific license,
all compatible with Apache License, Version 2. Please see individual files for details.


##  Credits and Acknowledgements

**Library Author:** Adrian Witas

