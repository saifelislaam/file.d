package modify

import (
	"sync"
	"testing"

	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/test"
	"github.com/stretchr/testify/assert"
)

func TestModify(t *testing.T) {
	config := test.NewConfig(&Config{"new_field": "new_value", "substitution_field": "${existing_field}"}, nil)
	p, input, output := test.NewPipelineMock(test.NewActionPluginStaticInfo(factory, config, pipeline.MatchModeAnd, nil, false))
	wg := &sync.WaitGroup{}
	wg.Add(1)

	outEvents := make([]*pipeline.Event, 0)
	output.SetOutFn(func(e *pipeline.Event) {
		outEvents = append(outEvents, e)
		wg.Done()
	})

	input.In(0, "test.log", 0, []byte(`{"existing_field":"existing_value"}`))

	wg.Wait()
	p.Stop()

	assert.Equal(t, 1, len(outEvents), "wrong out events count")
	assert.Equal(t, "new_value", outEvents[0].Root.Dig("new_field").AsString(), "wrong field value")
	assert.Equal(t, "existing_value", outEvents[0].Root.Dig("substitution_field").AsString(), "wrong field value")
}

func TestModifyRegex(t *testing.T) {
	testEvents := []struct {
		in           []byte
		fieldsValues map[string]string
	}{
		{
			[]byte(`{"existing_field":"existing_value"}`),
			map[string]string{
				"new_field":          "new_value",
				"substitution_field": "existing | value",
			},
		},
		{
			[]byte(`{"other_field":"other_value"}`),
			map[string]string{
				"new_field":          "new_value",
				"substitution_field": "",
			},
		},
	}

	config := test.NewConfig(&Config{
		"new_field":          "new_value",
		"substitution_field": "${existing_field|re(\"(existing).*(value)\", [1,2], \" | \")}",
	}, nil)
	p, input, output := test.NewPipelineMock(test.NewActionPluginStaticInfo(factory, config, pipeline.MatchModeAnd, nil, false))
	wg := &sync.WaitGroup{}

	outEvents := make([]*pipeline.Event, 0)
	output.SetOutFn(func(e *pipeline.Event) {
		outEvents = append(outEvents, e)
		wg.Done()
	})
	wg.Add(len(testEvents))

	for _, te := range testEvents {
		input.In(0, "test.log", 0, te.in)
	}

	wg.Wait()
	p.Stop()

	assert.Equal(t, len(testEvents), len(outEvents), "wrong out events count")
	for i := 0; i < len(testEvents); i++ {
		fvs := testEvents[i].fieldsValues
		for field := range fvs {
			assert.Equal(t, fvs[field], outEvents[i].Root.Dig(field).AsString(), "wrong field value")
		}
	}
}
