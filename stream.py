from typing import Any, Callable, List


def _initialize_target(
    transformations: List[Callable], sink: Callable, collector: Callable
):
    if transformations:
        target = transformations[0](transformations[1:], sink, collector)
        next(target)
    else:
        target = sink(collector)
        next(target)
    return target


class Stream:
    def __init__(self):
        self._source: Callable = None
        self._transformers: List[Callable] = []
        self._sink: Callable = None

    def __call__(self, input: List[Any], collector: Callable) -> None:
        """entry point to stream"""
        assert self._source is not None, "please set a source."
        assert len(self._transformers), "please set a transformer."
        assert self._sink is not None, "please set a sink."
        self._source(input, self._transformers, self._sink, collector)

    def source(self, f: Callable) -> Callable:
        """signature f: Any -> Iterable"""

        def wrapper(inp, transformations, sink, collector):
            target = _initialize_target(transformations, sink, collector)
            try:
                for i in f(inp):
                    target.send(i)
            except GeneratorExit:
                pass

        self._source = wrapper
        return wrapper

    def map_transform(self, f: Callable) -> Callable:
        """signature f: Any -> Any"""

        def wrapper(transformations, sink, collector):
            target = _initialize_target(transformations, sink, collector)
            try:
                while True:
                    rec = yield
                    target.send(f(rec))
            except GeneratorExit:
                pass

        self._transformers.append(wrapper)
        return wrapper

    def filter_transform(self, f: Callable) -> Callable:
        """signature f: Any -> bool"""

        def wrapper(transformations, sink, collector):
            target = _initialize_target(transformations, sink, collector)
            try:
                while True:
                    rec = yield
                    if f(rec):
                        target.send(rec)
            except GeneratorExit:
                pass

        self._transformers.append(wrapper)
        return wrapper

    def expand_transform(self, f: Callable) -> Callable:
        """signature f: Any -> Iterator"""

        def wrapper(transformations, sink, collector):
            target = _initialize_target(transformations, sink, collector)
            try:
                while True:
                    rec = yield
                    for i in f(rec):
                        target.send(i)
            except GeneratorExit:
                pass

        self._transformers.append(wrapper)
        return wrapper

    def reduce_transform(self, f: Callable) -> Callable:
        """signature f: Iterable -> Callable, Callable"""

        collect = []

        def wrapper(transformations, sink, collector):
            nonlocal collect
            target = _initialize_target(transformations, sink, collector)
            try:
                while True:
                    rec = yield
                    collect.append(rec)
                    condition, func = f(collect)
                    if condition():
                        target.send(func())
                        collect = []
                    else:
                        pass
            except GeneratorExit:
                pass

        self._transformers.append(wrapper)
        return wrapper

    def sink(self, f: Callable) -> Callable:
        """signature f: Any, Callable -> None"""

        def wrapper(collector: Callable) -> None:
            try:
                while True:
                    rec = yield
                    f(rec, collector)
            except GeneratorExit:
                pass

        self._sink = wrapper
        return wrapper
