"""Deterministic windows. These are sampled positions, not observed read/write indices."""


def positions(length, locals_, limits):
    head, tail = limits.get("sequence_head", 8), limits.get("sequence_tail", 8)
    selected = set(range(min(head, length)))
    selected.update(range(max(0, length - tail), length))
    windows = []
    radius = limits.get("sequence_radius", 2)
    for name in sorted(locals_):
        value = locals_[name]
        if type(value) is not int or not -length <= value < length:
            continue
        index = value % length
        window = list(range(max(0, index - radius), min(length, index + radius + 1)))
        selected.update(window)
        windows.append({"local": name, "value": value, "indices": window})
        if len(windows) >= limits.get("sequence_indices", 8):
            break
    return sorted(selected), windows
