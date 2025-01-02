def map(f, items):
    return [f(x) for x in items]

def _is_list(x):
    return type(x) == "list"

def flat_map(f, items):
    result = []
    for x in items:
        fx = f(x)
        result.extend(fx) if _is_list(fx) else result.append(fx)
    return result

def identity(x):
    return x

def flatten(items, max_depth = 1):
    """Flatten a list of items.
    see utils_tests.bzl for examples

    Args:
        items: the list to flatten
        max_depth: The maximum depth to flatten to
    Returns:
        a flattened list of items
    """
    result = items
    for i in range(max_depth):
        result = flat_map(identity, result)
    return result
