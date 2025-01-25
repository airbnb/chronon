from sample.joins.quickstart.training_set import source


def test_source():
    assert source.events.table == "data.checkouts"
 