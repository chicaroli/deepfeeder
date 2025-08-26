import sys
sys.path.append(r'c:\Users\chica\Quant\projects\deepfeeder\feeder_server\data\app.d')
from runtime.backfill import GapFiller

print('Loaded GapFiller OK')

class D(GapFiller):
    def __init__(self):
        super().__init__('p','s','k')
    def provider_gap_detection(self, dh_table, start=None, end=None):
        return []
    def provider_backfill_gaps(self, gaps):
        return None


d = D()
print('Coalesced:', d._coalesce_gaps([(10,20),(22,30),(100,200)], max_ids=1000, merge_distance=5))
