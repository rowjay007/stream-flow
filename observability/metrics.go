package observability

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func StartMetricsServer(addr string) error {
	http.Handle("/metrics", promhttp.Handler())
	return http.ListenAndServe(addr, nil)
}
