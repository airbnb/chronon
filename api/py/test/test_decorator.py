from ai.chronon.repo.run import retry_decorator

import pytest


def generator():
    """ Simple generator to have a changing variable. """
    for i in range(10):
        yield i


@pytest.fixture
def generated():
    """ Build fixture for generator so it can increment on subsequent calls """
    return generator()


def fail_x_times(generated, x):
    """ Function that will fail x times before succeeding given the same generator. """
    y = next(generated)
    if y < x:
        raise ValueError(f"Value was {y}")
    return y


@retry_decorator(1, 1)
def fail_x_times_and_succeed(generated, x):
    """ Decorated function that will succeed after failing once. """
    fail_x_times(generated, x)


def test_retry(generated):
    with pytest.raises(Exception):
        fail_x_times(generated, 3)
    fail_x_times_and_succeed(generated, 3)

