// Code generated from Pkl module `SegmentCofig`. DO NOT EDIT.
package segmentconfig

import (
	"context"

	"github.com/apple/pkl-go/pkl"
)

type SegmentCofig struct {
	MaxIndexBytes uint32 `pkl:"maxIndexBytes"`

	MaxStoreBytes uint32 `pkl:"maxStoreBytes"`

	InitialOffset uint32 `pkl:"initialOffset"`
}

// LoadFromPath loads the pkl module at the given path and evaluates it into a SegmentCofig
func LoadFromPath(ctx context.Context, path string) (ret *SegmentCofig, err error) {
	evaluator, err := pkl.NewEvaluator(ctx, pkl.PreconfiguredOptions)
	if err != nil {
		return nil, err
	}
	defer func() {
		cerr := evaluator.Close()
		if err == nil {
			err = cerr
		}
	}()
	ret, err = Load(ctx, evaluator, pkl.FileSource(path))
	return ret, err
}

// Load loads the pkl module at the given source and evaluates it with the given evaluator into a SegmentCofig
func Load(ctx context.Context, evaluator pkl.Evaluator, source *pkl.ModuleSource) (*SegmentCofig, error) {
	var ret SegmentCofig
	if err := evaluator.EvaluateModule(ctx, source, &ret); err != nil {
		return nil, err
	}
	return &ret, nil
}
