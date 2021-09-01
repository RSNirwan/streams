def producer(inps, ts, sink, collector):
    if ts:
        target = ts[0](ts[1:], sink, collector)
        next(target)
    else:
        target = sink(collector)
        next(target)
    try:
        for inp in inps:
            target.send(inp)
    except GeneratorExit:
        pass


def transformer(ts, sink, collector):
    if ts:
        target = ts[0](ts[1:], sink, collector)
        next(target)
    else:
        target = sink(collector)
        next(target)
    try:
        while True:
            rec = yield
            rec = rec * rec  # square numbers
            target.send(rec)
    except GeneratorExit:
        pass


def sink(collector):
    try:
        while True:
            rec = yield
            collector.append(rec)
    except GeneratorExit:
        pass


inps = [1, 2, 3, 4]
collector = []
producer(inps, [transformer], sink, collector)
print(collector)
