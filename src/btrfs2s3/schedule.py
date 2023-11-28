Path = Alias[Sequence[int]]
PathTuple = Alias[tuple[int, ...]]

def get_calendar_path(timestamp:float, tz:datetime.tzinfo) -> Path:
    dt_value = datetime.fromtimestamp(timestamp, tz=tz)
    return (dt_value.year, dt_value.month, dt_value.day, dt_value.hour,
            dt_value.minute, dt_value.second, dt_value.microsecond)


_T = TypeVar("_T")


def get_tree(Iterable[tuple[_T, Path]]) -> Mapping[PathTuple, Sequence[_T]]:
    tree :Mapping[PathTuple, Sequence[_T]] = collections.defaultdict(list)
    for key, path in inputs:
        for prefix_length in range(1, len(path) + 1):
            prefix = path[:prefix_length]
            tree[tuple(prefix)].append(key)
    return tree


def get_parent(
