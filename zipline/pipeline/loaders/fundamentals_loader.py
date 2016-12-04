from zipline.pipeline.loaders.base import PipelineLoader
from zipline.pipeline.loaders.frame import DataFrameLoader
from zipline.utils.calendars import get_calendar

class FundamentalsLoader(PipelineLoader):
    """
    Fundamental data loader.
    """
    def __init__(self, fundamentals_reader):
        self.fundamentals_reader = fundamentals_reader
        self._all_sessions = get_calendar("NYSE").all_sessions

    def load_adjusted_array(self, columns, dates, assets, mask):
        fundamentals_df = self.fundamentals_reader.read(
            [c.name for c in columns],
            dates,
            assets,
        )

        df_loader = DataFrameLoader(columns[0], fundamentals_df)

        return df_loader.load_adjusted_array(columns, dates, assets, mask)
