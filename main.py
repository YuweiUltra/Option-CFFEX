import pandas as pd
from enums import ExchangeTypes
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from config import config_backtest_id
from copy import deepcopy
from tqdm import tqdm

from ExchangeSimulator import Base_Exchange
from Broker import Base_Broker
from Strategy import Base_Strategy
import warnings
import os

warnings.filterwarnings("ignore")


class Exchange(Base_Exchange):
    def __init__(self, exchange_symbol, trading_calender, exchange_type, start_date, end_date, future_data,
                 backtest_ids):
        super().__init__(exchange_symbol, trading_calender, exchange_type, start_date, end_date)
        self.cached_data = None
        self.pre_price_data = None
        self.future_data = future_data  # Pass future_data into the Exchange class
        self.backtest_ids = backtest_ids  # Backtest IDs passed to the Exchange

    def request_data(self, method='hist'):
        if method == 'hist':
            if self.cached_data is None:
                # Cache data to avoid repeated disk I/O
                self.cached_data = pd.read_csv('CleanedData_options.csv', index_col=0)
            return self.cached_data

    def process_contracts(self, price_data, trading_date):
        # Generate month and contract identifiers
        trading_date_timestamp = pd.Timestamp(trading_date)
        curr_month = trading_date_timestamp.strftime('%y%m')
        next_month = (trading_date_timestamp + pd.DateOffset(months=1)).strftime('%y%m')
        next_next_month = (trading_date_timestamp + pd.DateOffset(months=2)).strftime('%y%m')

        # Generate lists of backtest contract IDs for each month
        curr_month_list = [id + curr_month for id in self.backtest_ids]
        next_month_list = [id + next_month for id in self.backtest_ids]
        next_next_month_list = [id + next_next_month for id in self.backtest_ids]

        # Filter future contracts for the relevant months
        future_contracts = self.future_data.loc[
            self.future_data.index.isin(curr_month_list + next_month_list + next_next_month_list, level=1)
        ].loc[trading_date]

        # Merge option and future contracts to get combined data
        option_contracts = pd.merge(left=price_data, right=future_contracts, left_on='underlying_id',
                                    right_index=True, suffixes=('', '_underlying'))

        # Filter only put options ('P')
        option_contracts = option_contracts[option_contracts['option_type'] == 'P']

        # Group by underlying_id and filter contracts for sell and buy based on strike price
        sell_contracts = option_contracts.groupby('underlying_id').apply(
            lambda group: group[group['strike_price'] > group['close_underlying'].iloc[0]]
        ).reset_index(level=0, drop=True).sort_values(by=['underlying_id', 'strike_price'], ascending=[True, True])

        buy_contracts = option_contracts.groupby('underlying_id').apply(
            lambda group: group[group['strike_price'] < group['close_underlying'].iloc[0]]
        ).reset_index(level=0, drop=True).sort_values(by=['underlying_id', 'strike_price'], ascending=[True, False])

        return sell_contracts, buy_contracts, option_contracts

    def __next__(self):
        if self.current_idx >= len(self.trading_calender):
            raise StopIteration

        # Set the current trading time
        self._curr_trading_time = self.trading_calender[self.current_idx]
        self.current_idx += 1

        # Request price data for the current day
        price_data = self.request_data()

        # Ingest the data into the exchange
        self.ingest(price_data)

        # If both backtest info and data are available, process contracts
        if self._backtest_activate_info and self._backtest_activate_data:
            sell_contracts, buy_contracts, option_contracts = self.process_contracts(self.curr_price_df,
                                                                                     self.curr_trading_time)
            self._curr_price_df = option_contracts

            # Return the current trading time, info, price data, and filtered contracts
            return self.curr_trading_time, self.curr_info_df, self.curr_price_df, sell_contracts, buy_contracts


class Broker(Base_Broker):
    def __init__(self, init_cash, exchange):
        super().__init__(init_cash, exchange)

    def buy_option(self, option_id, quantity):
        try:
            try:
                price = self.exchange.curr_price_df.loc[option_id, 'close']
                strike = self.exchange.curr_price_df.loc[option_id, 'strike_price']
                de_listed_date = self.exchange.curr_price_df.loc[option_id, 'de_listed_date']
                entry_price = self.exchange.curr_price_df.loc[option_id, 'close_underlying']
            except:
                price = self.exchange.pre_price_data.loc[option_id, 'close']
                strike = self.exchange.pre_price_data.loc[option_id, 'strike_price']
                de_listed_date = self.exchange.pre_price_data.loc[option_id, 'de_listed_date']
                entry_price = self.exchange.pre_price_data.loc[option_id, 'close_underlying']
        except:
            price = self.positions[option_id]['avg_price']
            strike = int(option_id.split('-')[-1])
            de_listed_date = 0
            entry_price = 0

        total_cost = price * quantity * 100
        commission = strike * quantity * 0.00
        total_cost += commission

        if option_id in self.positions:
            if self.positions[option_id]['shares'] < 0:  # If the position is a short
                short_position = abs(self.positions[option_id]['shares'])
                if quantity <= short_position:
                    self.cash -= total_cost
                    self.positions[option_id]['shares'] += quantity
                    if self.positions[option_id]['shares'] == 0:
                        del self.positions[option_id]
                else:
                    raise ValueError("Trying to buy more than what was shorted.")
            else:
                # Regular long position
                self.cash -= total_cost
                self.positions[option_id]['shares'] += quantity
                self.positions[option_id]['avg_price'] = (
                                                                 (self.positions[option_id]['avg_price'] *
                                                                  self.positions[option_id]['shares']) + total_cost
                                                         ) / (self.positions[option_id]['shares'] + quantity)
        else:
            self.cash -= total_cost
            self.positions[option_id] = {
                'shares': quantity,
                'avg_price': price,
                'de_listed_date': de_listed_date,
                'entry_price': entry_price
            }

        self.orders.append({
            'action': 'buy',
            'option_id': option_id,
            'quantity': quantity,
            'price': price,
            'date': self.curr_trading_time
        })

    def sell_option(self, option_id, quantity):
        try:
            try:
                price = self.exchange.curr_price_df.loc[option_id, 'close']
                strike = self.exchange.curr_price_df.loc[option_id, 'strike_price']
                de_listed_date = self.exchange.curr_price_df.loc[option_id, 'de_listed_date']
                entry_price = self.exchange.curr_price_df.loc[option_id, 'close_underlying']
            except:
                price = self.exchange.pre_price_data.loc[option_id, 'close']
                strike = self.exchange.pre_price_data.loc[option_id, 'strike_price']
                de_listed_date = self.exchange.pre_price_data.loc[option_id, 'de_listed_date']
                entry_price = self.exchange.pre_price_data.loc[option_id, 'close_underlying']
        except:
            price = self.positions[option_id]['avg_price']
            strike = int(option_id.split('-')[-1])
            de_listed_date = 0
            entry_price = 0

        total_revenue = price * quantity * 100  # Contract size is 100 units per option
        commission = strike * quantity * 0.00  # 0.3% transaction fee
        total_revenue -= commission

        if option_id in self.positions:
            if self.positions[option_id]['shares'] > 0:
                if self.positions[option_id]['shares'] >= quantity:
                    self.positions[option_id]['shares'] -= quantity
                    self.cash += total_revenue
                    if self.positions[option_id]['shares'] == 0:
                        del self.positions[option_id]
                else:
                    raise ValueError("Trying to sell more than current long position.")
            else:
                self.positions[option_id]['shares'] -= quantity
                self.cash += total_revenue
        else:
            self.positions[option_id] = {
                'shares': -quantity,
                'avg_price': price,
                'de_listed_date': de_listed_date,
                'entry_price': entry_price
            }
            self.cash += total_revenue

        self.orders.append({
            'action': 'sell',
            'option_id': option_id,
            'quantity': quantity,
            'price': price,
            'date': self.curr_trading_time
        })

    def close_all_positions(self):
        for option_id in list(self.positions.keys()):
            position = self.positions[option_id]
            if position['shares'] > 0:
                self.sell_option(option_id, position['shares'])
            else:
                self.buy_option(option_id, abs(position['shares']))

    def update_portfolio_value(self):
        total_value = self.cash
        premium_value = 0  # Represents the total cost of all open options
        nominal_value = 0  # Represents the notional value of the underlying assets

        for option_id, position in self.positions.items():
            try:
                market_price = self.exchange.curr_price_df.loc[option_id]['close']
                strike_price = self.exchange.curr_price_df.loc[option_id]['strike_price']
            except:
                market_price = self.positions[option_id]['avg_price']
                strike_price = int(option_id.split('-')[-1])
            # TODO: 名义本金和权利金计算

            if position['shares'] > 0:  # Long position
                total_value += market_price * position['shares'] * 100
                premium_value += position['avg_price'] * position['shares'] * 100
                nominal_value += strike_price * position['shares'] * 100
            else:  # Short position
                total_value -= market_price * abs(position['shares']) * 100
                premium_value -= position['avg_price'] * abs(position['shares']) * 100
                nominal_value += strike_price * abs(position['shares']) * 100

        self.portfolio_value = total_value
        self.premium_value = premium_value
        self.nominal_value = nominal_value


buy = 0
sell = 2
buy_far = 0


# TODO: like this buy sell 档, create 远近月份档

class Strategy(Base_Strategy):
    def __init__(self, broker, exchange):
        super().__init__(broker, exchange)
        self.future_data = pd.read_csv('CleanedData_futures.csv', index_col=0).set_index(['date', 'uni_id'])
        self.__results = []
        self.__last_portfolio_value = broker.portfolio_value

    def execute_trade(self, sell_contract_id, buy_contract_id, buy_contract_id_far):
        # self.broker.buy_option(buy_contract_id_far, 1)
        self.broker.buy_option(sell_contract_id, 1)
        self.broker.sell_option(buy_contract_id, 2)

    def __next__(self):
        trading_date, market_info, price_data, sell_contracts, buy_contracts = next(self.exchange)
        trading_date_timestamp = pd.Timestamp(trading_date)
        event = None

        ############################ during trading ############################

        if not self.broker.positions:
            if not sell_contracts.empty and not buy_contracts.empty:
                sell_contract_id = sell_contracts.index[sell]
                buy_contract_id = buy_contracts.index[buy]
                buy_contract_id_far = sell_contracts.index[buy_far]
                self.execute_trade(sell_contract_id, buy_contract_id, buy_contract_id_far)

        else:
            if not sell_contracts.empty and not buy_contracts.empty:
                for option_id in list(self.broker.positions.keys()):
                    position = self.broker.positions[option_id]
                    days_to_expiry = (
                            pd.to_datetime(position['de_listed_date']) - trading_date_timestamp).days
                    atm_strike = self.exchange.curr_price_df.loc[
                        option_id, 'close_underlying'] if option_id in self.exchange.curr_price_df.index else position[
                        'entry_price']

                    if days_to_expiry <= 5:
                        event = '移仓换月'
                        self.broker.close_all_positions()

                        underlying_ids = sell_contracts.underlying_id.unique()
                        underlying_ids.sort()

                        try:
                            sell_contract_id = \
                                sell_contracts[sell_contracts['underlying_id'] == underlying_ids[1]].index[sell]
                            buy_contract_id = \
                                buy_contracts[buy_contracts['underlying_id'] == underlying_ids[1]].index[
                                    buy]
                            buy_contract_id_far = \
                                sell_contracts[sell_contracts['underlying_id'] == underlying_ids[1]].index[buy_far]
                        except:
                            sell_contract_id = \
                                sell_contracts[sell_contracts['underlying_id'] == underlying_ids[0]].index[sell]
                            buy_contract_id = \
                                buy_contracts[buy_contracts['underlying_id'] == underlying_ids[0]].index[
                                    buy]
                            buy_contract_id_far = \
                                sell_contracts[sell_contracts['underlying_id'] == underlying_ids[0]].index[buy_far]

                        self.execute_trade(sell_contract_id, buy_contract_id, buy_contract_id_far)
                        break

                    # elif (atm_strike - position['entry_price']) / position['entry_price'] >= 0.05:
                    #     event = '上涨超过百分之五'
                    #     self.broker.close_all_positions()
                    #     if not sell_contracts.empty and not buy_contracts.empty:
                    #         sell_contract_id = sell_contracts.index[sell]
                    #         buy_contract_id = buy_contracts.index[buy]
                    #         try:
                    #             buy_contract_id_far = sell_contracts.index[buy_far]
                    #         except:
                    #             buy_contract_id_far = sell_contracts.index[]
                    #         self.execute_trade(sell_contract_id, buy_contract_id, buy_contract_id_far)
                    #     break

        ############################ after trading ############################
        self.exchange.pre_price_data = price_data
        self.broker.update_portfolio_value()
        portfolio_value = self.broker.portfolio_value
        try:
            daily_return = (portfolio_value - self.__last_portfolio_value) / self.broker.nominal_value
        except:
            daily_return = 0
        self.__last_portfolio_value = portfolio_value

        # Log transactions for the current trading day
        transactions = []
        for order in self.broker.orders:
            if order['date'] == trading_date and order['action'] in ['buy', 'sell']:
                transactions.append({
                    'option_id': order['option_id'],
                    'action': order['action'],
                    'quantity': order['quantity'],
                    'price': order['price'],
                    'date': order['date']
                })

        # Store daily results
        self.__results.append({
            'date': trading_date,
            'portfolio_value': portfolio_value,
            'cash': self.broker.cash,
            'positions': deepcopy(self.broker.positions),
            'transactions': transactions,
            'daily_return': daily_return,
            'event': event
        })

    def run(self):
        for _ in tqdm(self, total=len(self.exchange.trading_calender)):
            pass
            # print('-' * 40)
            # print(f"PROCESSING DATE:  {self.exchange.curr_trading_time}")
            # print(f"PORTFOLIO CASH {self.broker.cash}")
            # print(f"PORTFOLIO VALUE {self.broker.portfolio_value}")
            # print(f"PORTFOLIO positions {self.broker.positions}")

        # Convert results to DataFrame
        results_df = pd.DataFrame(self.__results)
        results_df.set_index('date', inplace=True)
        return results_df


option_data = pd.read_csv('CleanedData_options.csv', index_col=0)
future_data = pd.read_csv('CleanedData_futures.csv', index_col=0)
trading_calender = option_data['date'].unique()
trading_calender.sort()

start_date = '2022-09-01'
end_date = '2024-09-30'
ZJS_Exchange = Exchange('ZJS', trading_calender, ExchangeTypes.Option, start_date, end_date,
                        future_data.set_index(['date', 'uni_id']),
                        config_backtest_id)
ZJS_Broker = Broker(init_cash=0, exchange=ZJS_Exchange)
ZJS_Strategy = Strategy(broker=ZJS_Broker, exchange=ZJS_Exchange)

results_df = ZJS_Strategy.run()
results_df['cumulative_return'] = (1 + results_df['daily_return']).cumprod() - 1


########################plot##############################################
def p_lines_multicol(positions_df, h=400, w=800):
    """
    Create a multi-column line plot for asset positions over time.

    Parameters:
    - positions_df: DataFrame containing asset positions with dates as the index.
    - h: Height of the plot.
    - w: Width of the plot.

    Returns:
    - fig: The Plotly figure object.
    """
    fig = go.Figure()

    # Create a line trace for each asset
    for asset in positions_df.columns:
        fig.add_trace(go.Scatter(
            x=positions_df.index,
            y=positions_df[asset],
            mode='lines+markers',
            name=asset,
            showlegend=True  # Show legend for asset positions
        ))

    # Customize the layout
    fig.update_layout(
        title='Asset Positions Over Time',
        xaxis_title='Date',
        yaxis_title='Shares Held',
        height=h,
        width=w,
        xaxis=dict(type='date'),
        legend_title='Assets'
    )

    return fig


def plot_all(results_df):
    results_dir = './plots/temp'

    try:
        os.makedirs(results_dir, exist_ok=True)
    except OSError as e:
        print(f"Error creating directory {results_dir}: {e}")
        return

    # Convert positions to a DataFrame where each column is an asset and values are numeric quantities held
    positions_list = results_df['positions'].tolist()
    positions_df = pd.DataFrame([{k: v['shares'] for k, v in day.items()} for day in positions_list],
                                index=results_df.index)

    # Create a subplot figure
    fig = make_subplots(rows=2, cols=2, subplot_titles=(
        'Asset Positions', 'Portfolio Value', 'Cumulative Returns', 'Underlying Cumulative Return'))

    # Add asset positions plot
    for asset in positions_df.columns:
        fig.add_trace(go.Scatter(
            x=positions_df.index,
            y=positions_df[asset],
            mode='lines+markers',
            name=asset,
            showlegend=True  # Show legend for asset positions
        ), row=1, col=1)

    # Add vertical lines for events
    for idx, row in results_df.iterrows():
        if row['event'] == '移仓换月':
            fig.add_vline(x=idx, line_color='red', line_width=2, opacity=0.7, row=1, col=1)
        elif row['event'] == '上涨超过百分之五':
            fig.add_vline(x=idx, line_color='yellow', line_width=2, opacity=0.7, row=1, col=1)

    # Add portfolio value plot
    fig.add_trace(go.Scatter(
        x=results_df.index,
        y=results_df['portfolio_value'],
        mode='lines',
        name='Portfolio Value',
        showlegend=True  # Show legend for portfolio value
    ), row=1, col=2)

    fig.add_trace(go.Scatter(
        x=results_df.index,
        y=results_df['cash'],
        mode='lines',
        name='cash',
        showlegend=True  # Show legend for portfolio value
    ), row=1, col=2)

    # Add cumulative returns plot
    fig.add_trace(go.Scatter(
        x=results_df.index,
        y=results_df['cumulative_return'],
        mode='lines',
        name='Cumulative Return',
        showlegend=True  # Show legend for cumulative return
    ), row=2, col=1)

    # Add underlying cumulative return plot
    fig.add_trace(go.Scatter(
        x=results_df.index,
        y=results_df['underlying_cumulative_return'],
        mode='lines',
        name='Underlying Cumulative Return',
        showlegend=True  # Show legend for underlying cumulative return
    ), row=2, col=2)

    # Customize layout for the overall figure
    fig.update_layout(
        title="Investment Strategy Overview",
        xaxis_title="Date",
        height=800,
        width=1400,
        showlegend=True
    )

    return fig


# Prepare your data (assuming future_data and config_backtest_id are defined)
filtered_future_data = future_data[future_data['uni_id'].str.startswith(config_backtest_id[0])]
sorted_future_data = (filtered_future_data
                      .sort_values(by=['date', 'uni_id'])
                      .groupby('date', as_index=False)
                      .first())

merged_results = pd.merge(sorted_future_data[['date', 'close', 'uni_id']],
                          results_df,
                          left_on='date',
                          right_index=True,
                          how='right')

merged_results['underlying_return'] = merged_results['close'].pct_change()
merged_results['underlying_cumulative_return'] = (1 + merged_results['underlying_return']).cumprod() - 1
merged_results = merged_results.set_index('date')

# Create and show the combined plot
fig = plot_all(merged_results)
fig.show()
# fig.write_html("plot_figure_50.html")
