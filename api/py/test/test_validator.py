"""
Forcing validator to fail some tests
"""

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

import pytest
from ai.chronon.repo import validator


@pytest.fixture
def zvalidator():
    return validator.ChrononRepoValidator(chronon_root_path="api/py/test/sample", output_root="production")


@pytest.fixture
def valid_online_join(zvalidator):
    return sorted(
        [join for join in zvalidator.old_joins if join.metaData.online is True], key=lambda x: x.metaData.name
    )[0]


@pytest.fixture
def valid_online_group_by(valid_online_join):
    return sorted(
        [jp.groupBy for jp in valid_online_join.joinParts if jp.groupBy.metaData.online is True],
        key=lambda x: x.metaData.name,
    )[0]


@pytest.fixture
def valid_events_group_by(zvalidator):
    return sorted(
        [
            jp.groupBy
            for join in zvalidator.old_joins
            for jp in join.joinParts
            if any([src.events is not None for src in jp.groupBy.sources])
        ],
        key=lambda x: x.metaData.name,
    )[0]


def test_validate_group_by_online(zvalidator, valid_online_group_by):
    # No errors on regular run.
    assert not zvalidator._validate_group_by(valid_online_group_by)
    valid_online_group_by.metaData.online = False
    assert zvalidator._validate_group_by(valid_online_group_by)


def test_validate_group_by_prod_on_prod_join(zvalidator, valid_online_group_by, valid_online_join):
    assert not zvalidator._validate_group_by(valid_online_group_by)
    valid_online_join.metaData.production = True
    valid_online_group_by.metaData.production = False
    assert zvalidator._validate_group_by(valid_online_group_by)


def test_validate_group_by_prod_promotes_on_prod_join(zvalidator, valid_online_group_by, valid_online_join):
    assert not zvalidator._validate_group_by(valid_online_group_by)
    valid_online_join.metaData.production = True
    valid_online_group_by.metaData.production = None
    assert not zvalidator._validate_group_by(valid_online_group_by)
    assert valid_online_group_by.metaData.production is True


def test_validate_join_prod_join_non_prod_group_by(zvalidator, valid_online_join, valid_online_group_by):
    assert not zvalidator._validate_join(valid_online_join)
    valid_online_join.metaData.production = True
    valid_online_group_by.metaData.production = False
    assert zvalidator._validate_join(valid_online_join)


def test_validate_join_online_join_offline_group_by(zvalidator, valid_online_join, valid_online_group_by):
    assert not zvalidator._validate_join(valid_online_join)
    valid_online_group_by.metaData.online = False
    assert zvalidator._validate_join(valid_online_join)


def test_validate_obj_on_non_relevant(zvalidator):
    assert zvalidator.validate_obj("Not an object") == []


def test_can_skip_materialize_if_offline(zvalidator, valid_online_group_by):
    assert not zvalidator.can_skip_materialize(valid_online_group_by)
    valid_online_group_by.metaData.online = False
    valid_online_group_by.metaData.production = False
    assert zvalidator.can_skip_materialize(valid_online_group_by)


def test_can_skip_materialize_if_not_used(zvalidator, valid_online_group_by):
    assert not zvalidator.can_skip_materialize(valid_online_group_by)
    valid_online_group_by.metaData.production = None
    valid_online_group_by.metaData.online = None
    for join in list(zvalidator._get_old_joins_with_group_by(valid_online_group_by)):
        join.metaData.online = False
    assert zvalidator.can_skip_materialize(valid_online_group_by)


def test_validate_cumulative_source_no_timequery(zvalidator, valid_events_group_by):
    assert len(zvalidator._validate_group_by(valid_events_group_by)) == 0
    valid_events_group_by.sources[0].events.isCumulative = True
    valid_events_group_by.sources[0].events.query.timeColumn = None
    assert len(zvalidator._validate_group_by(valid_events_group_by)) > 0


def test_validate_group_by_with_incorrect_derivations(zvalidator):
    from sample.group_bys.sample_team.sample_group_by_with_incorrect_derivations import (
        v1,
    )

    errors = zvalidator._validate_group_by(v1)
    assert len(errors) > 0


def test_validate_group_by_with_derivations(zvalidator):
    from sample.group_bys.sample_team.sample_group_by_with_derivations import v1

    errors = zvalidator._validate_group_by(v1)
    assert len(errors) == 0


def test_validate_join_with_derivations(zvalidator):
    from sample.joins.sample_team.sample_join_derivation import v1

    errors = zvalidator._validate_join(v1)
    assert len(errors) == 0


def test_validate_join_with_derivations_on_external_parts(zvalidator):
    from sample.joins.sample_team.sample_join_with_derivations_on_external_parts import (
        v1,
    )

    errors = zvalidator._validate_join(v1)
    assert len(errors) == 0


def test_validate_group_by_deprecation_date(zvalidator):
    from sample.group_bys.sample_team.sample_deprecation_group_by import (
        v1,
        v1_incorrect_deprecation_format,
    )

    errors = zvalidator._validate_group_by(v1)
    assert len(errors) == 0
    errors = zvalidator._validate_group_by(v1_incorrect_deprecation_format)
    assert len(errors) == 1


# Derive from external column to key should be enabled in validator
def test_validate_derivation_on_keys(zvalidator):
    from sample.joins.sample_team.sample_join_external_parts import v2

    errors = zvalidator._validate_join(v2)
    assert len(errors) == 0, f"Failed on: {errors}"
