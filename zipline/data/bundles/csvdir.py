"""
Module for building a complete dataset from local directory with csv files.
"""
import os
import sys

import logbook
from numpy import empty
from pandas import Series, DataFrame, read_csv, Index, Timedelta, NaT

from zipline.utils.calendars import register_calendar_alias
from zipline.utils.cli import maybe_show_progress

logger = logbook.Logger(__name__)

def csvdir_equities(tframe='daily', start=None, end=None):
    """
    Generate an ingest function for custom data bundle

    Parameters
    ----------
    tframe: string, optional
        The data time frame ('minute', 'daily')
    start : datetime, optional
        The start date to query for. By default this pulls the full history
        for the calendar.
    end : datetime, optional
        The end date to query for. By default this pulls the full history
        for the calendar.
    Returns
    -------
    ingest : callable
        The bundle ingest function for the given set of symbols.
    Examples
    --------
    This code should be added to ~/.zipline/extension.py
    .. code-block:: python
       from zipline.data.bundles import csvdir_equities, register
       register('custom-csvdir-bundle', csvdir_equities(sys.environ['CSVDIR'], 'minute'))

    Notes
    -----
    Environment variable CSVDIR must contain path to the directory with <symbol>.cvs files
    sids for each symbol will be the index into the symbols sequence.
    """

    return CSVDIRBundle(tframe, start, end).ingest

class CSVDIRBundle:
    """
    Wrapper class to enable write access to self.splits and self.dividends
    from _pricing_iter method.
    """

    def __init__(self, tframe, start, end):
        self.tframe = tframe
        self.start = start
        self.end = end

        self.show_progress = None
        self.symbols = None
        self.metadata = None
        self.csvdir = None

        self.splits = DataFrame()
        self.dividends = DataFrame()
        self.fundamentals_writer = None

    def ingest(self, environ, asset_db_writer, minute_bar_writer, daily_bar_writer,
               adjustment_writer, fundamentals_writer, calendar, start_session,
               end_session, cache, show_progress, output_dir):

        csvdir = os.environ.get('CSVDIR')
        if not csvdir:
            logger.error("CSVDIR environment variable is not set")
            sys.exit(1)
        if not os.path.isdir(csvdir):
            logger.error("%s is not a directory" % csvdir)
            sys.exit(1)

        self.symbols = sorted(item.split('.csv')[0] for item in os.listdir(csvdir) \
                                                    if item.endswith('.csv'))
        if not self.symbols:
            logger.error("no <symbol>.csv files found in %s" % csvdir)
            sys.exit(1)

        self.csvdir = csvdir

        self.metadata = DataFrame(empty(len(self.symbols),
                                        dtype=[('start_date', 'datetime64[ns]'),
                                               ('end_date', 'datetime64[ns]'),
                                               ('auto_close_date', 'datetime64[ns]'),
                                               ('symbol', 'object')]))

        self.show_progress = show_progress

        writer = minute_bar_writer if self.tframe == 'minute' else daily_bar_writer
        writer.write(self._pricing_iter(), show_progress=show_progress)

        # Hardcode the exchange to "CSVDIR" for all assets and (elsewhere)
        # register "CSVDIR" to resolve to the NYSE calendar, because these are
        # all equities and thus can use the NYSE calendar.
        self.metadata['exchange'] = "CSVDIR"
        asset_db_writer.write(equities=self.metadata)

        adjustment_writer.write(splits=self.splits, dividends=self.dividends)

    def _pricing_iter(self):
        with maybe_show_progress(self.symbols, self.show_progress,
                                 label='Loading custom pricing data: ') as it:
            for sid, symbol in enumerate(it):
                logger.debug('%s: sid %s' % (symbol, sid))

                dfr = read_csv(os.path.join(self.csvdir, '%s.csv' % symbol),
                               parse_dates=[0], infer_datetime_format=True,
                               index_col=0).sort_index()

                # the start date is the date of the first trade and
                # the end date is the date of the last trade
                start_date = dfr.index[0]
                end_date = dfr.index[-1]

                # The auto_close date is the day after the last trade.
                ac_date = end_date + Timedelta(days=1)
                self.metadata.iloc[sid] = start_date, end_date, ac_date, symbol

                if 'split' in dfr.columns:
                    tmp = dfr[dfr['split'] != 1.0]['split']
                    split = DataFrame(data=tmp.index.tolist(), columns=['effective_date'])
                    split['ratio'] = tmp.tolist()
                    split['sid'] = sid

                    index = Index(range(self.splits.shape[0],
                                        self.splits.shape[0] + split.shape[0]))
                    split.set_index(index, inplace=True)
                    self.splits = self.splits.append(split)

                if 'dividend' in dfr.columns:
                    # ex_date   amount  sid record_date declared_date pay_date
                    tmp = dfr[dfr['dividend'] != 0.0]['dividend']
                    div = DataFrame(data=tmp.index.tolist(), columns=['ex_date'])
                    div['record_date'] = NaT
                    div['declared_date'] = NaT
                    div['pay_date'] = NaT
                    div['amount'] = tmp.tolist()
                    div['sid'] = sid
                    index = Index(range(self.dividends.shape[0],
                                        self.dividends.shape[0] + div.shape[0]))
                    div.set_index(index, inplace=True)
                    self.dividends = self.dividends.append(div)

                yield sid, dfr

register_calendar_alias("CSVDIR", "NYSE")