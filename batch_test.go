package batcher

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type mockStorage struct {
	buf []any
}

func (ms *mockStorage) insert(_ context.Context, buf []any) error {
	ms.buf = append(ms.buf, buf...)
	return nil
}

func TestBatchInserter(t *testing.T) {
	t.Run("flush working correctly", func(t *testing.T) {
		flushInterval := time.Second * 3
		ms := mockStorage{}
		bufCap := 100
		minAcceptableRowsNum := 1
		b := NewBatchInserter(context.Background(), flushInterval, ms.insert, bufCap, minAcceptableRowsNum, zap.NewNop())

		wg := new(sync.WaitGroup)
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				b.Append(i)
			}(i)
		}

		wg.Wait()
		require.Eventually(t, func() bool { return len(ms.buf) == 100 }, time.Second*6, time.Millisecond*200)
	})
}
