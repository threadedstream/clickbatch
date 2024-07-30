# clickbatch

## Usage 

First, download it
```
go get github.com/threadedstream/clickbatch
```

```go
  import (
    "github.com/threadedstream/clickbatch
  )

  func insertCallback(ctx context.Context, buf []any) error {
    conn, err := // get clickhouse connection
    batch, err := conn.PrepareBatch(...)
    for i := 0; i < len(buf); i += columnsNum {
      		if err = batch.Append(buf[i : i+fieldsLen]...); err != nil {
      			// log error
			      return batch.Abort()
          }
		}
    ...
    return nil
  }
  func main() {
    batchInserter := batcher.NewBatchInserter(
		    ctx,
		    batchInsertFlushInterval, insertCallback,
		    bufferCapacity, batchMinAcceptableRowsNum,
		    logger,
    )

    batchInserter.Append(val1, val2, val3, val4)
    ...
  }
```

## How does it work
As you know Clickhouse isn't good with frequent inserts, so there's a need to buffer your inserts somewhere and send them in the form of a single batch. 
Here's where clickbatch shines -- it allows you to buffer inserted values internally and periodically flush these values to Clickhouse. 
Most importantly, it frees you from responsibility to roll your own implementation of batcher, which is lovely.
