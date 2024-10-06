from abc import ABC, abstractmethod
import pandas as pd


class ExchangeSimulator(ABC):
    def __init__(self, exchange_symbol, trading_calender, exchange_type, start_date, end_date):
        """
        Provide exchange name to create an exchange simulator.
        A trading calendar for the exchange is required.
        """
        self._start_date = start_date
        self._end_date = end_date
        self._exchange_symbol = exchange_symbol
        self._trading_calender = trading_calender[(trading_calender >= start_date) & (trading_calender <= end_date)]
        self._exchange_type = exchange_type
        self._backtest_activate_info = False
        self._backtest_activate_data = False
        self._current_idx = 0

    @property
    def start_date(self):
        return self._start_date

    @property
    def end_date(self):
        return self._end_date

    @property
    def exchange_symbol(self):
        return self._exchange_symbol

    @property
    def trading_calender(self):
        return self._trading_calender

    @property
    def exchange_type(self):
        return self._exchange_type

    @property
    def current_idx(self):
        return self._current_idx

    @current_idx.setter
    def current_idx(self, value):
        self._current_idx = value

    @abstractmethod
    def ingest(self, data: pd.DataFrame):
        pass

    @abstractmethod
    def request_data(self, curr_trading_time, method):
        pass

    def __iter__(self):
        return self

    def __next__(self):
        if self._current_idx >= len(self._trading_calender):
            raise StopIteration
        curr_trading_time = self._trading_calender[self._current_idx]
        self._current_idx += 1
        return curr_trading_time


class Base_Exchange(ExchangeSimulator):
    def __init__(self, exchange_symbol, trading_calender, exchange_type, start_date, end_date):
        super().__init__(exchange_symbol, trading_calender, exchange_type, start_date, end_date)
        self._curr_trading_time = None
        self._curr_info_df = None
        self._curr_price_df = None

    @property
    def curr_trading_time(self):
        return self._curr_trading_time

    @property
    def curr_info_df(self):
        return self._curr_info_df

    @property
    def curr_price_df(self):
        return self._curr_price_df

    def ingest(self, data: pd.DataFrame):
        required_cols = ['uni_id', 'date', 'exchange', 'type', 'listed_date', 'de_listed_date']
        missing_cols = [col for col in required_cols if col not in data.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")

        if not (data['type'] == self.exchange_type.value).all():
            raise ValueError("Data type mismatch")
        if not (data['exchange'] == self.exchange_symbol).all():
            raise ValueError("Exchange symbol mismatch")

        data['date'] = pd.to_datetime(data['date'])
        data['listed_date'] = pd.to_datetime(data['listed_date'])
        data['de_listed_date'] = pd.to_datetime(data['de_listed_date'])

        filtered_data = data.drop_duplicates()
        filtered_data = filtered_data[(filtered_data['date'] == self.curr_trading_time)]
        filtered_data = filtered_data[
            (filtered_data['listed_date'] <= self.curr_trading_time) &
            (filtered_data['de_listed_date'] > self.curr_trading_time)
            ]

        if not filtered_data['uni_id'].is_unique:
            raise ValueError("Duplicate uni_id entries found")

        if not filtered_data.empty:
            self._curr_info_df = filtered_data[
                ['uni_id', 'exchange', 'type', 'listed_date', 'de_listed_date']].set_index('uni_id')
            self._curr_price_df = filtered_data.drop(
                columns=['date', 'exchange', 'type', 'listed_date', 'de_listed_date']).set_index('uni_id')
            self._backtest_activate_info = True
            self._backtest_activate_data = True

    def request_data(self, method='hist'):
        if method == 'hist':
            return pd.DataFrame()
        elif method == 'live':
            return pd.DataFrame()
        return pd.DataFrame()

    def __next__(self):
        if self.current_idx >= len(self.trading_calender):
            raise StopIteration
        self._curr_trading_time = self.trading_calender[self.current_idx]
        self.current_idx += 1
        data = self.request_data()
        self.ingest(data)
        if self._backtest_activate_info and self._backtest_activate_data:
            return self.curr_trading_time, self.curr_info_df, self.curr_price_df
