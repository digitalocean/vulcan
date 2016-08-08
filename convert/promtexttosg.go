package convert

import (
	"fmt"
	"io"
	"time"

	"github.com/digitalocean/vulcan/bus"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
)

// PromTextToSG reads prometheus metrics in text exposition format and
// produces a SampleGroup
func PromTextToSG(in io.Reader) (bus.SampleGroup, error) {
	fam := []*dto.MetricFamily{}
	dec := expfmt.NewDecoder(in, expfmt.FmtText)
	for {
		var f dto.MetricFamily
		err := dec.Decode(&f)
		if err != nil {
			if err == io.EOF {
				break
			}
			return bus.SampleGroup{}, err
		}
		fam = append(fam, &f)
	}
	sg := familyToSampleGroup(fam)
	return sg, nil
}

func familyToSampleGroup(metricFamilies []*dto.MetricFamily) bus.SampleGroup {
	samples := bus.SampleGroup{}
	for _, mf := range metricFamilies {
		for _, prom := range mf.GetMetric() {
			m := bus.Metric{
				Name:   mf.GetName(),
				Labels: map[string]string{},
			}
			for _, label := range prom.GetLabel() {
				m.Labels[label.GetName()] = label.GetValue()
			}
			switch mf.GetType() {
			case dto.MetricType_COUNTER:
				s := bus.Sample{
					Metric: m,
					Datapoint: bus.Datapoint{
						Value:     prom.Counter.GetValue(),
						Timestamp: promTimestamp(prom),
					},
				}
				s.Metric.Labels["__type__"] = "counter"
				samples = append(samples, &s)
			case dto.MetricType_GAUGE:
				s := bus.Sample{
					Metric: m,
					Datapoint: bus.Datapoint{
						Value:     prom.Gauge.GetValue(),
						Timestamp: promTimestamp(prom),
					},
				}
				s.Metric.Labels["__type__"] = "gauge"
				samples = append(samples, &s)
			case dto.MetricType_HISTOGRAM:
				// add bucket values, sum, and count for this prometheus metric
				m.Labels["__type__"] = "histogram"
				bucketName := m.Name + "_bucket"
				for _, b := range prom.Histogram.Bucket {
					myMetric := cloneMetric(m)
					myMetric.Name = bucketName
					myMetric.Labels["le"] = fmt.Sprint(b.GetUpperBound())
					s := bus.Sample{
						Metric: myMetric,
						Datapoint: bus.Datapoint{
							Value:     float64(b.GetCumulativeCount()),
							Timestamp: promTimestamp(prom),
						},
					}
					samples = append(samples, &s)
				}
				// do sum
				sum := cloneMetric(m)
				sum.Name = sum.Name + "_sum"
				s := bus.Sample{
					Metric: sum,
					Datapoint: bus.Datapoint{
						Value:     float64(*prom.Histogram.SampleSum),
						Timestamp: promTimestamp(prom),
					},
				}
				samples = append(samples, &s)
				// do count
				count := cloneMetric(m)
				count.Name = count.Name + "_count"
				s = bus.Sample{
					Metric: count,
					Datapoint: bus.Datapoint{
						Value:     float64(*prom.Histogram.SampleCount),
						Timestamp: promTimestamp(prom),
					},
				}
				samples = append(samples, &s)
			case dto.MetricType_SUMMARY:
				m.Labels["__type__"] = "summary"
				for _, q := range prom.Summary.Quantile {
					myMetric := cloneMetric(m)
					myMetric.Labels["quantile"] = fmt.Sprint(q.GetQuantile())
					s := bus.Sample{
						Metric: myMetric,
						Datapoint: bus.Datapoint{
							Value:     float64(q.GetValue()),
							Timestamp: promTimestamp(prom),
						},
					}
					samples = append(samples, &s)
				}
				// do sum
				sum := cloneMetric(m)
				sum.Name = sum.Name + "_sum"
				s := bus.Sample{
					Metric: sum,
					Datapoint: bus.Datapoint{
						Value:     float64(*prom.Summary.SampleSum),
						Timestamp: promTimestamp(prom),
					},
				}
				samples = append(samples, &s)
				// do count
				count := cloneMetric(m)
				count.Name = count.Name + "_count"
				s = bus.Sample{
					Metric: count,
					Datapoint: bus.Datapoint{
						Value:     float64(*prom.Summary.SampleCount),
						Timestamp: promTimestamp(prom),
					},
				}
				samples = append(samples, &s)
			}
		}
	}
	return samples
}

func promTimestamp(m *dto.Metric) bus.Timestamp {
	if m.TimestampMs != nil {
		return bus.Timestamp(*m.TimestampMs)
	}
	return bus.Timestamp(time.Now().UnixNano() / 1e6)
}

func cloneMetric(m bus.Metric) bus.Metric {
	result := bus.Metric{
		Name:   m.Name,
		Labels: map[string]string{},
	}
	for k, v := range m.Labels {
		result.Labels[k] = v
	}
	return result
}
