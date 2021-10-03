def producer(inps, ts, sink, collector):
    if ts:
        target = ts[0](ts[1:], sink, collector)
        next(target)
    else:
        target = sink(collector)
        next(target)
    try:
        for inp in inps:
            print(f"producer: sending {inp} to target.")
            trec = target.send(inp)
            print(f"producer: received {trec} from target.")
    except GeneratorExit:
        pass


def transformer(ts, sink, collector):
    if ts:
        target = ts[0](ts[1:], sink, collector)
        next(target)
    else:
        target = sink(collector)
        next(target)
    trec = None
    try:
        while True:
            rec = yield trec
            print(f"transformer: received {rec}.")
            rec = rec * rec  # square numbers
            print(f"transformer: sending {rec} to target.")
            trec = target.send(rec)
            print(f"transformer: received {trec} from target.")
    except GeneratorExit:
        pass


def sink(collector):
    trec = None
    try:
        while True:
            rec = yield trec
            print(f"sink: received {rec}.")
            collector.append(rec)
            trec = "success"
    except GeneratorExit:
        pass


inps = [1, 2, 3, 4]
collector = []
producer(inps, [transformer], sink, collector)
print(collector)
