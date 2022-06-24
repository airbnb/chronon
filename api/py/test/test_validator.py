"""
Forcing validator to fail some tests
"""
import pytest

from ai.chronon.repo import validator


@pytest.fixture
def zvalidator():
    return validator.ChrononRepoValidator(
        chronon_root_path='test/sample',
        output_root='production'
    )


@pytest.fixture
def valid_online_join(zvalidator):
    return sorted([
        join for join in zvalidator.old_joins if join.metaData.online is True
    ], key=lambda x: x.metaData.name)[0]


@pytest.fixture
def valid_online_group_by(valid_online_join):
    return sorted([
        jp.groupBy for jp in valid_online_join.joinParts if jp.groupBy.metaData.online is True
    ], key=lambda x: x.metaData.name)[0]


@pytest.fixture
def valid_events_group_by(zvalidator):
    return sorted([
        jp.groupBy for join in zvalidator.old_joins for jp in join.joinParts
        if any([src.events is not None for src in jp.groupBy.sources])
    ], key=lambda x: x.metaData.name)[0]


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
