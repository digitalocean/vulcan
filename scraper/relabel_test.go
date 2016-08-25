// Copyright 2016 The Vulcan Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package scraper

import (
	"reflect"
	"sort"
	"testing"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/model"
)

var toString = func(p *string) string {
	return *p
}

var toStrPtr = func(s string) *string {
	return &s
}

type byName []*dto.LabelPair

func (lp byName) Len() int      { return len(lp) }
func (lp byName) Swap(i, j int) { lp[i], lp[j] = lp[j], lp[i] }
func (lp byName) Less(i, j int) bool {
	return toString(lp[i].Name) < toString(lp[j].Name)
}

func TestToLabelPair(t *testing.T) {
	var happyPathTests = []struct {
		desc     string
		arg      model.LabelSet
		expected []*dto.LabelPair
	}{
		{
			desc: "3 values",
			arg: model.LabelSet{
				"a": "value_a",
				"b": "value_b",
				"c": "value_c",
			},
			expected: []*dto.LabelPair{
				&dto.LabelPair{
					Name:  toStrPtr("a"),
					Value: toStrPtr("value_a"),
				},
				&dto.LabelPair{
					Name:  toStrPtr("b"),
					Value: toStrPtr("value_b"),
				},
				&dto.LabelPair{
					Name:  toStrPtr("c"),
					Value: toStrPtr("value_c"),
				},
			},
		},
		{
			desc: "1 values",
			arg: model.LabelSet{
				"a": "value_a",
			},
			expected: []*dto.LabelPair{
				&dto.LabelPair{
					Name:  toStrPtr("a"),
					Value: toStrPtr("value_a"),
				},
			},
		},
		{
			desc:     "0 values",
			arg:      model.LabelSet{},
			expected: []*dto.LabelPair{},
		},
		{
			desc: "26 values",
			arg: model.LabelSet{
				"a": "value_a",
				"b": "value_b",
				"c": "value_c",
				"d": "value_d",
				"e": "value_e",
				"f": "value_f",
				"g": "value_g",
				"h": "value_h",
				"i": "value_i",
				"j": "value_j",
				"k": "value_k",
				"l": "value_l",
				"m": "value_m",
				"n": "value_n",
				"o": "value_o",
				"p": "value_p",
				"q": "value_q",
				"r": "value_r",
				"s": "value_s",
				"t": "value_t",
				"u": "value_u",
				"v": "value_v",
				"w": "value_w",
				"x": "value_x",
				"y": "value_y",
				"z": "value_z",
			},
			expected: []*dto.LabelPair{
				&dto.LabelPair{
					Name:  toStrPtr("a"),
					Value: toStrPtr("value_a"),
				},
				&dto.LabelPair{
					Name:  toStrPtr("b"),
					Value: toStrPtr("value_b"),
				},
				&dto.LabelPair{
					Name:  toStrPtr("c"),
					Value: toStrPtr("value_c"),
				},
				&dto.LabelPair{
					Name:  toStrPtr("d"),
					Value: toStrPtr("value_d"),
				},
				&dto.LabelPair{
					Name:  toStrPtr("e"),
					Value: toStrPtr("value_e"),
				},
				&dto.LabelPair{
					Name:  toStrPtr("f"),
					Value: toStrPtr("value_f"),
				},
				&dto.LabelPair{
					Name:  toStrPtr("g"),
					Value: toStrPtr("value_g"),
				},
				&dto.LabelPair{
					Name:  toStrPtr("h"),
					Value: toStrPtr("value_h"),
				},
				&dto.LabelPair{
					Name:  toStrPtr("i"),
					Value: toStrPtr("value_i"),
				},
				&dto.LabelPair{
					Name:  toStrPtr("j"),
					Value: toStrPtr("value_j"),
				},
				&dto.LabelPair{
					Name:  toStrPtr("k"),
					Value: toStrPtr("value_k"),
				},
				&dto.LabelPair{
					Name:  toStrPtr("l"),
					Value: toStrPtr("value_l"),
				},
				&dto.LabelPair{
					Name:  toStrPtr("m"),
					Value: toStrPtr("value_m"),
				},
				&dto.LabelPair{
					Name:  toStrPtr("n"),
					Value: toStrPtr("value_n"),
				},
				&dto.LabelPair{
					Name:  toStrPtr("o"),
					Value: toStrPtr("value_o"),
				},
				&dto.LabelPair{
					Name:  toStrPtr("p"),
					Value: toStrPtr("value_p"),
				},
				&dto.LabelPair{
					Name:  toStrPtr("q"),
					Value: toStrPtr("value_q"),
				},
				&dto.LabelPair{
					Name:  toStrPtr("r"),
					Value: toStrPtr("value_r"),
				},
				&dto.LabelPair{
					Name:  toStrPtr("s"),
					Value: toStrPtr("value_s"),
				},
				&dto.LabelPair{
					Name:  toStrPtr("t"),
					Value: toStrPtr("value_t"),
				},
				&dto.LabelPair{
					Name:  toStrPtr("u"),
					Value: toStrPtr("value_u"),
				},
				&dto.LabelPair{
					Name:  toStrPtr("v"),
					Value: toStrPtr("value_v"),
				},
				&dto.LabelPair{
					Name:  toStrPtr("w"),
					Value: toStrPtr("value_w"),
				},
				&dto.LabelPair{
					Name:  toStrPtr("x"),
					Value: toStrPtr("value_x"),
				},
				&dto.LabelPair{
					Name:  toStrPtr("y"),
					Value: toStrPtr("value_y"),
				},
				&dto.LabelPair{
					Name:  toStrPtr("z"),
					Value: toStrPtr("value_z"),
				},
			},
		},
	}

	for i, test := range happyPathTests {
		t.Logf("happy path tests %d: %q", i, test.desc)

		got := toLabelPair(test.arg)

		sort.Sort(byName(got))
		sort.Sort(byName(test.expected))

		if !reflect.DeepEqual(got, test.expected) {
			t.Errorf(
				"toLabelPair(%v) => got %v; expected %v",
				test.arg,
				got,
				test.expected,
			)
		}
	}
}

func TestToLabelSet(t *testing.T) {
	var happyPathTest = []struct {
		desc     string
		arg      []*dto.LabelPair
		expected model.LabelSet
	}{
		{
			desc: "3 values",
			arg: []*dto.LabelPair{
				&dto.LabelPair{
					Name:  toStrPtr("a"),
					Value: toStrPtr("value_a"),
				},
				&dto.LabelPair{
					Name:  toStrPtr("b"),
					Value: toStrPtr("value_b"),
				},
				&dto.LabelPair{
					Name:  toStrPtr("c"),
					Value: toStrPtr("value_c"),
				},
			},
			expected: model.LabelSet{
				"a": "value_a",
				"b": "value_b",
				"c": "value_c",
			},
		},
		{
			desc: "1 values",
			arg: []*dto.LabelPair{
				&dto.LabelPair{
					Name:  toStrPtr("a"),
					Value: toStrPtr("value_a"),
				},
			},
			expected: model.LabelSet{
				"a": "value_a",
			},
		},
		{
			desc:     "0 values",
			arg:      []*dto.LabelPair{},
			expected: model.LabelSet{},
		},
		{
			desc: "26 values",
			arg: []*dto.LabelPair{
				&dto.LabelPair{
					Name:  toStrPtr("a"),
					Value: toStrPtr("value_a"),
				},
				&dto.LabelPair{
					Name:  toStrPtr("b"),
					Value: toStrPtr("value_b"),
				},
				&dto.LabelPair{
					Name:  toStrPtr("c"),
					Value: toStrPtr("value_c"),
				},
				&dto.LabelPair{
					Name:  toStrPtr("d"),
					Value: toStrPtr("value_d"),
				},
				&dto.LabelPair{
					Name:  toStrPtr("e"),
					Value: toStrPtr("value_e"),
				},
				&dto.LabelPair{
					Name:  toStrPtr("f"),
					Value: toStrPtr("value_f"),
				},
				&dto.LabelPair{
					Name:  toStrPtr("g"),
					Value: toStrPtr("value_g"),
				},
				&dto.LabelPair{
					Name:  toStrPtr("h"),
					Value: toStrPtr("value_h"),
				},
				&dto.LabelPair{
					Name:  toStrPtr("i"),
					Value: toStrPtr("value_i"),
				},
				&dto.LabelPair{
					Name:  toStrPtr("j"),
					Value: toStrPtr("value_j"),
				},
				&dto.LabelPair{
					Name:  toStrPtr("k"),
					Value: toStrPtr("value_k"),
				},
				&dto.LabelPair{
					Name:  toStrPtr("l"),
					Value: toStrPtr("value_l"),
				},
				&dto.LabelPair{
					Name:  toStrPtr("m"),
					Value: toStrPtr("value_m"),
				},
				&dto.LabelPair{
					Name:  toStrPtr("n"),
					Value: toStrPtr("value_n"),
				},
				&dto.LabelPair{
					Name:  toStrPtr("o"),
					Value: toStrPtr("value_o"),
				},
				&dto.LabelPair{
					Name:  toStrPtr("p"),
					Value: toStrPtr("value_p"),
				},
				&dto.LabelPair{
					Name:  toStrPtr("q"),
					Value: toStrPtr("value_q"),
				},
				&dto.LabelPair{
					Name:  toStrPtr("r"),
					Value: toStrPtr("value_r"),
				},
				&dto.LabelPair{
					Name:  toStrPtr("s"),
					Value: toStrPtr("value_s"),
				},
				&dto.LabelPair{
					Name:  toStrPtr("t"),
					Value: toStrPtr("value_t"),
				},
				&dto.LabelPair{
					Name:  toStrPtr("u"),
					Value: toStrPtr("value_u"),
				},
				&dto.LabelPair{
					Name:  toStrPtr("v"),
					Value: toStrPtr("value_v"),
				},
				&dto.LabelPair{
					Name:  toStrPtr("w"),
					Value: toStrPtr("value_w"),
				},
				&dto.LabelPair{
					Name:  toStrPtr("x"),
					Value: toStrPtr("value_x"),
				},
				&dto.LabelPair{
					Name:  toStrPtr("y"),
					Value: toStrPtr("value_y"),
				},
				&dto.LabelPair{
					Name:  toStrPtr("z"),
					Value: toStrPtr("value_z"),
				},
			},
			expected: model.LabelSet{
				"a": "value_a",
				"b": "value_b",
				"c": "value_c",
				"d": "value_d",
				"e": "value_e",
				"f": "value_f",
				"g": "value_g",
				"h": "value_h",
				"i": "value_i",
				"j": "value_j",
				"k": "value_k",
				"l": "value_l",
				"m": "value_m",
				"n": "value_n",
				"o": "value_o",
				"p": "value_p",
				"q": "value_q",
				"r": "value_r",
				"s": "value_s",
				"t": "value_t",
				"u": "value_u",
				"v": "value_v",
				"w": "value_w",
				"x": "value_x",
				"y": "value_y",
				"z": "value_z",
			},
		},
	}

	for i, test := range happyPathTest {
		t.Logf("happy path tests %d: %q", i, test.desc)

		got := toLabelSet(test.arg)

		if !reflect.DeepEqual(got, test.expected) {
			t.Errorf(
				"toLabelSet(%v) => got %v; expected %v",
				test.arg,
				got,
				test.expected,
			)
		}
	}
}
