package xsuportal

import (
	"log"
	"net/http"
	"os"

	"cloud.google.com/go/profiler"
	"contrib.go.opencensus.io/exporter/stackdriver"
	"contrib.go.opencensus.io/integrations/ocsql"
	"go.opencensus.io/plugin/ochttp"
	"go.opencensus.io/trace"
)

var httpClient = &http.Client{
	Transport: &ochttp.Transport{},
}

func InitProfiler(name string) {
	//hostname, _ := os.Hostname()
	//if hostname != "isu01" {
	//        return
	//}
	if err := profiler.Start(profiler.Config{
		Service:        "isucon10f-" + name,
		ServiceVersion: "1.0.0",
		ProjectID:      os.Getenv("GOOGLE_CLOUD_PROJECT"),
	}); err != nil {
		log.Fatal(err)
	}
}

func InitTrace() {
	//hostname, _ := os.Hostname()
	//if hostname != "isu01" {
	//        return
	//}
	exporter, err := stackdriver.NewExporter(stackdriver.Options{
		ProjectID:                os.Getenv("GOOGLE_CLOUD_PROJECT"),
		TraceSpansBufferMaxBytes: 32 * 1024 * 1024,
	})
	if err != nil {
		log.Fatal(err)
	}
	trace.RegisterExporter(exporter)

	trace.ApplyConfig(trace.Config{DefaultSampler: trace.ProbabilitySampler(0.05)})
}

func WithTrace(h http.Handler) http.Handler {
	return &ochttp.Handler{Handler: h}
}

func tracedDriver(driverName string) string {
	driverName, err := ocsql.Register(driverName, ocsql.WithQuery(true), ocsql.WithQueryParams(true))
	if err != nil {
		log.Fatal(err)
	}
	return driverName
}
