
#     Copyright (C) 2023 The Chronon Authors.
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.

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

