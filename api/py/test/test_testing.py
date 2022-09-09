from ai.chronon.repo import testing


def test_main(fake_process):
    from joins.sample_team.sample_join import v1
    fake_process.allow_unregistered(True)
    fake_process.register(["bash", fake_process.any()])
    testing.run_test_config('sample_team', v1)
