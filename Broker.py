from enums import OrderTypes, AssetTypes
from abc import ABC, abstractmethod
import math
import pandas as pd


class Broker(ABC):
    @abstractmethod
    def __init__(self, init_cash: float, exchange: 'Exchange') -> None:
        self._init_cash = init_cash
        self._portfolio_value = init_cash
        self._cash = init_cash
        self._exchange = exchange
        self._curr_trading_time = exchange.curr_trading_time
        self._positions = {}  # Store positions as a dictionary {uni_id: {'shares': int, 'avg_price': float}}
        self._orders = []
        self._transactions = []
        self._returns = []

    @property
    def init_cash(self) -> float:
        return self._init_cash

    @property
    def cash(self) -> float:
        return self._cash

    @cash.setter
    def cash(self, value: float) -> None:
        self._cash = value

    @property
    def portfolio_value(self) -> float:
        return self._portfolio_value

    @portfolio_value.setter
    def portfolio_value(self, value: float) -> None:
        self._portfolio_value = value

    @property
    def curr_trading_time(self) -> pd.Timestamp:
        return self._exchange.curr_trading_time

    @property
    def positions(self) -> dict:
        return self._positions

    @property
    def orders(self) -> list:
        return self._orders

    @property
    def transactions(self) -> list:
        return self._transactions

    @property
    def exchange(self) -> 'Exchange':
        return self._exchange


class Base_Broker(Broker):
    def __init__(self, init_cash: float, exchange: 'Exchange') -> None:
        super().__init__(init_cash, exchange)
        self._premium_value = 0.0
        self._nominal_value = 0.0

    @property
    def nominal_value(self) -> float:
        return self._nominal_value

    @nominal_value.setter
    def nominal_value(self, value: float) -> None:
        self._nominal_value = value

    @property
    def premium_value(self) -> float:
        return self._premium_value

    @premium_value.setter
    def premium_value(self, value: float) -> None:
        self._premium_value = value

    def buy_option(self, option_id: str, quantity: int) -> None:
        # Implement logic for buying options
        pass

    def sell_option(self, option_id: str, quantity: int) -> None:
        # Implement logic for selling options
        pass

    def update_portfolio(self) -> None:
        # Implement logic for updating the portfolio
        pass
