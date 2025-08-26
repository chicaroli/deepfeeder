import pytest

from runtime.backfill import GapFiller


class DummyFiller(GapFiller):
    def __init__(self):
        super().__init__(provider='x', symbol='t', key_column='k')

    def provider_gap_detection(self, dh_table, start=None, end=None):
        raise NotImplementedError()

    def provider_backfill_gaps(self, gaps):
        raise NotImplementedError()


def test_coalesce_simple():
    g = DummyFiller()
    gaps = [(10, 20), (22, 30), (100, 200)]
    merged = g._coalesce_gaps(gaps, max_ids=1000, merge_distance=5)
    # first two should merge because distance 22 <= 20 + 5
    assert (10, 30) in merged
    assert (100, 200) in merged


def test_coalesce_split_on_max_ids():
    g = DummyFiller()
    # small start..large end forces split
    gaps = [(0, 200_000)]
    merged = g._coalesce_gaps(gaps, max_ids=50_000, merge_distance=1)
    # Should be split into multiple chunks
    assert len(merged) > 1
    # chunks should be approximate size
    for s, e in merged:
        assert (e - s) <= 50_000
