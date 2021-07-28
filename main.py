from numbers import Number
from typing import Callable, List, Union, Iterator
from stream import Stream

stream = Stream()


@stream.source
def source(inp: Union[Number, List[Number]]) -> Iterator:
    if isinstance(inp, Number):
        yield inp
    else:
        yield from inp


@stream.filter_transform
def transform1(inp: Number) -> bool:
    # forward only even numbers
    return (inp - 1) % 2


@stream.expand_transform
def transform2(inp: Number) -> Iterator:
    # tripple every input
    yield from [inp, inp, inp]


@stream.map_transform
def transform3(inp: Number) -> Number:
    # square inp
    return inp * inp


@stream.reduce_transform
def transform4(inps: List[Number]) -> Number:
    # sum every two numbers
    condition = lambda: len(inps) >= 2
    func = lambda: sum(inps)
    return condition, func


@stream.sink
def sink(inp: Number, collector: Callable) -> None:
    collector(inp)


a = []
collector = lambda x: a.append(x)
stream(input=[1, 2, 3], collector=collector)
print(a)

stream(input=4, collector=collector)
stream(input=5, collector=collector)
print(a)

for i in [6, 7, 8]:
    stream(input=i, collector=collector)
print(a)
