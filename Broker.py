from enums import OrderTypes, AssetTypes
from abc import ABC, abstractmethod
import math
import pandas as pd


class Broker(ABC):
    @abstractmethod
    def __init__(self, init_cash, exchange):
        self.__init_cash = init_cash
        self.__portfolio_value = init_cash
        self.__cash = init_cash
        self.__exchange = exchange
        self.__curr_trading_time = exchange.curr_trading_time
        self.__positions = {}  # Store positions as a dictionary {uni_id: {'shares': int, 'avg_price': float}}
        self.__orders = []
        self.__transactions = []
        self.__returns = []

    @property
    def init_cash(self):
        return self.__init_cash

    @property
    def cash(self):
        return self.__cash

    @property
    def portfolio_value(self):
        return self.__portfolio_value

    @property
    def curr_trading_time(self):
        return self.__exchange.curr_trading_time

    @property
    def positions(self):
        return self.__positions

    @property
    def orders(self):
        return self.__orders

    @property
    def transactions(self):
        return self.__transactions

    @property
    def exchange(self):
        return self.__exchange


class Base_Broker(Broker):
    def __init__(self, init_cash, exchange):
        super().__init__(init_cash, exchange)
        self.__premium_value = 0
        self.__nominal_value = 0

    @property
    def nominal_value(self):
        return self.__nominal_value

    @property
    def premium_value(self):
        return self.__premium_value

    def update_value(self):
        """
        根据 position 和 market_price update
        在每个交易时间结束之后计算一次.

        检查持仓中是否有期权的 de_listed_dates 是今天，
        如果是，则判断 underlyingclose 和 strike_price 的关系，
        考虑期权是看涨还是看跌，得到期权的实值，
        变成现金并将该 position 删除。
        """

        position_value = 0
        nominal_value = 0
        market_price = self.exchange.curr_price_df

        # 合约到期
        for uni_id, pos in list(self.positions.items()):
            de_listed_date = pd.Timestamp(pos['de_listed_date'])

            if pos['type'] == AssetTypes.Option:

                if de_listed_date == self.curr_trading_time:
                    # Option is expiring, calculate its intrinsic value
                    strike_price = self.exchange.assets_df.loc[
                        self.exchange.assets_df['uni_ids'] == uni_id, 'strike_price'].values[0]
                    option_type = self.exchange.assets_df.loc[
                        self.exchange.assets_df['uni_ids'] == uni_id, 'option_type'].values[0]
                    underlying_close = pos['underlyingclose']

                    intrinsic_value = 0
                    if option_type == 'C':  # Call option
                        intrinsic_value = max(0, underlying_close - strike_price)
                    elif option_type == 'P':  # Put option
                        intrinsic_value = max(0, strike_price - underlying_close)

                    # Cash settlement for the intrinsic value
                    cash_settlement = intrinsic_value * pos['shares'] * pos['multiplier']
                    self.__cash += cash_settlement

                    # Remove the position since it expired
                    del self.__positions[uni_id]

                else:
                    # Regular update for non-expiring options
                    price = market_price.loc[uni_id, 'close']
                    pos['curr_price'] = price
                    pos['curr_value'] = pos['shares'] * price * pos['multiplier']
                    position_value += pos['curr_value']
                    pos['premium_value'] = pos['curr_value']

                    pos['underlyingclose'] = market_price.loc[uni_id, 'underlyingclose']
                    pos['nominal_value'] = pos['shares'] * pos['multiplier'] * pos['underlyingclose']
                    nominal_value -= pos['nominal_value']

            elif pos['type'] == AssetTypes.Future:

                if de_listed_date == self.exchange.curr_trading_time:
                    # Cash settlement
                    price = pos['curr_price']
                    cash_settlement = (pos['shares'] * (price - pos['avg_price']) * pos['multiplier'] +
                                       pos['shares'] * pos['avg_price'] * pos['multiplier'] * pos['margin_ratio'])
                    self.__cash += cash_settlement

                    # Remove the position since it expired
                    del self.__positions[uni_id]

                else:
                    # Regular update for non-expiring options
                    price = market_price[market_price.underlying_order_book_id == uni_id].underlyingclose[0]
                    pos['curr_price'] = price
                    pos['curr_value'] = (pos['shares'] * (price - pos['avg_price']) * pos['multiplier'] +
                                         pos['shares'] * pos['avg_price'] * pos['multiplier'] * pos['margin_ratio'])

                    position_value += pos['curr_value']
                    # pos['premium_value_futures'] = pos['curr_value']
                    pos['nominal_value'] = pos['shares'] * pos['multiplier'] * price
                    nominal_value += pos['nominal_value']

        # Update the portfolio value with remaining positions and cash
        self.__portfolio_value = position_value + self.__cash
        self.__premium_value = position_value
        self.__nominal_value = nominal_value

    def custom_execute_option(self, order):
        market_data = self.exchange.curr_price_df.loc[order['uni_id']]
        price = (market_data.open + market_data.close) / 2
        shares = order['shares']
        if self.cash > 0:
            shares = min(shares, math.floor(self.cash / price / order['multiplier']))
        else:
            shares = 0

        if shares <= 0:
            order['status'] = OrderTypes.Cancelled
            return 0

        if price * shares <= self.cash:
            order['status'] = OrderTypes.Filled
            order['shares'] = shares

        # update positions
        if order['uni_id'] in self.positions:
            old_shares = self.positions[order['uni_id']]['shares']
            old_avg_price = self.positions[order['uni_id']]['avg_price']
            new_shares = old_shares + shares
            new_avg_price = (old_shares * old_avg_price + shares * price) / new_shares
        else:
            new_shares = shares
            new_avg_price = price

        value = price * shares * order['multiplier']
        info = self.exchange.assets_df.set_index('uni_ids').loc[order['uni_id']]
        self.positions[order['uni_id']] = {'shares': new_shares,
                                           'avg_price': new_avg_price,
                                           'multiplier': order['multiplier'],
                                           'value': value,
                                           'type': order['type'],
                                           'de_listed_date': info.de_listed_dates,
                                           'underlying_order_book_id': info.underlying_order_book_id,
                                           'underlyingclose': market_data.underlyingclose}

        # update cash
        self.__cash -= value
        return shares

    def custom_execute_future(self, order):

        df = self.exchange.curr_price_df
        price = df[df.underlying_order_book_id == order['uni_id']].underlyingclose[0]
        shares = order['shares']
        multiplier = order['multiplier']
        margin_ratio = order['margin']

        price_one_contract = price * multiplier
        cash_one_contract = price * multiplier * margin_ratio

        if self.cash > 0:
            shares = min(shares, math.floor(self.cash / cash_one_contract))
        else:
            shares = 0

        if shares <= 0:
            order['status'] = OrderTypes.Cancelled
            return

        if cash_one_contract * shares <= self.cash:  # value
            order['status'] = OrderTypes.Filled
            order['shares'] = shares

        # for future
        if order['uni_id'] in self.positions:
            old_shares = self.positions[order['uni_id']]['shares']
            old_avg_price = self.positions[order['uni_id']]['avg_price']
            new_shares_futures = old_shares + shares
            new_avg_price_futures = (old_shares * old_avg_price + shares * price) / new_shares_futures
        else:
            new_shares_futures = shares
            new_avg_price_futures = price

        # for future
        value = price * shares * order['multiplier'] * margin_ratio
        self.positions[order['uni_id']] = {'shares': new_shares_futures,
                                           'avg_price': new_avg_price_futures,
                                           'multiplier': multiplier,
                                           'value': value,
                                           'type': AssetTypes.Future,
                                           'margin_ratio': margin_ratio,
                                           'de_listed_date': self.exchange.curr_datas_df[
                                               self.exchange.curr_datas_df.underlying_order_book_id ==
                                               order['uni_id']].maturity_date.iloc[0], }

        # update cash # for future
        self.__cash -= value

        return shares

    def order_target_shares(self, order):
        order['order_id'] = len(self.__orders) + 1
        order['trading_time'] = self.curr_trading_time
        self.__orders.append(order)

        if order['type'] == AssetTypes.Option:
            shares = self.custom_execute_option(order)
        elif order['type'] == AssetTypes.Future:
            shares = self.custom_execute_future(order)
        return shares
