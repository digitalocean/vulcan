# compressor

## algorithm

```pseudocode
chunks = map[metricID]struct{
    chunk []byte
    oldestOffset int64
    oldestTimestamp int64
}
messages = kafka.ReadPartitionFrom(kafka.LastCommittedOffset)
for message in messages {
    samples = excludeSamplesPastLastKnownTimestamp(message)
    full = appendSamplesToChunks(chunks, samples)
    idle = removeIdle(chunks)
    aged = removeAged(chunks)
    flush(full, idle, aged)
    kafka.commitOffset(oldestActiveOffset(chunks)) 
}

appendSamplesToChunks(chunks, message) {
    exclude samples
}
```

Is it possible for us to double-write data in this scenario ?

a   a a  a a   a  a a  a a a   a a  a a  a a    a  a  a a   a a  a a  a  a  a a  a    a   a a  a a
b b b    b  b  b    b    b b b b b    b  b b    b   b  b b b    b b   b  b b b    b   b   b b b   b
 c      c                                                                                          
d            d                d               d              d               d             d      d

flushes by line (every 5 points, or becomes idle, or aged)
           a             a            a               a               a               a  
            b                b             b               b               b                b  
                        c[nil no longer active]
                     d                               d                    d                    d

minimum flush point (c = committed offset, t = trigger)
c----------t
c-----------t
c--------------------t
c-----------------------t                     
            c------------t
                     c-------t
