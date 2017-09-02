package main

import (
	"net/http"
	"os"

	"github.com/go-kit/kit/log"
	minio "github.com/minio/minio-go"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/metric"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/prometheus/tsdb/labels"
)

func startServer(configFile string) {
	logger := log.NewLogfmtLogger(os.Stdout)

	rcfg, err := loadConfig(configFile)
	if err != nil {
		logger.Log("error", err.Error())
		return
	}

	mc, err := minio.New(rcfg.Endpoint, rcfg.AccessKey, rcfg.SecretKey, rcfg.UseSSL)
	if err != nil {
		logger.Log("error", err.Error())
		return
	}

	db, err := NewDB(rcfg, mc, log.With(logger, "component", "db"))
	if err != nil {
		logger.Log("error", err.Error())
		return
	}

	http.HandleFunc("/read", func(w http.ResponseWriter, r *http.Request) {
		req, err := remote.DecodeReadRequest(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		results := make([]*remote.QueryResult, len(req.Queries))
		for i, q := range req.Queries {
			mat, err := queryToMatrix(q, db)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			results[i] = remote.ToQueryResult(mat)
		}

		resp := remote.ReadResponse{
			Results: results,
		}

		if err := remote.EncodReadResponse(&resp, w); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}

		return
	})

	http.ListenAndServe(":9091", nil)
}

func queryToMatrix(rq *remote.Query, db *DB) (model.Matrix, error) {
	mint, maxt, matchers, err := remote.FromQuery(rq)
	if err != nil {
		return nil, err
	}

	q := db.Querier(int64(mint), int64(maxt))

	ms := make([]labels.Matcher, 0, len(matchers))
	for _, m := range matchers {
		switch m.Type {
		case metric.Equal:
			ms = append(ms, labels.NewEqualMatcher(string(m.Name), string(m.Value)))

		case metric.NotEqual:
			ms = append(ms, labels.Not(labels.NewEqualMatcher(string(m.Name), string(m.Value))))

		case metric.RegexMatch:
			rm, err := labels.NewRegexpMatcher(string(m.Name), string(m.Value))
			if err != nil {
				return nil, err
			}
			ms = append(ms, rm)

		case metric.RegexNoMatch:
			rm, err := labels.NewRegexpMatcher(string(m.Name), string(m.Value))
			if err != nil {
				return nil, err
			}
			ms = append(ms, labels.Not(rm))
		default:
			return nil, errors.New("new Matcher came out of nowhere")
		}
	}

	matrix := make(model.Matrix, 0)
	ss := q.Select(ms...)
	for ss.Next() {
		s := ss.At()
		sstream := &model.SampleStream{
			Metric: getMetricFromLabels(s.Labels()),
			Values: make([]model.SamplePair, 0),
		}

		it := s.Iterator()
		for it.Next() {
			t, v := it.At()
			sstream.Values = append(sstream.Values, model.SamplePair{
				Timestamp: model.Time(t),
				Value:     model.SampleValue(v),
			})
		}

		if err := it.Err(); err != nil {
			return nil, err
		}

		matrix = append(matrix, sstream)
	}
	if err := ss.Err(); err != nil {
		return nil, err
	}

	return matrix, nil
}

func getMetricFromLabels(ls labels.Labels) model.Metric {
	m := make(model.Metric)
	for _, l := range ls {
		m[model.LabelName(l.Name)] = model.LabelValue(l.Value)
	}

	return m
}