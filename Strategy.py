import pandas as pd


class Base_Strategy:
    def __init__(self, broker, exchange):
        """
        exchange is a market simulator
        which generates trading_date, market_info, market_price
        """
        self.__exchange = exchange
        self.__broker = broker
        self.__results = []
        self.__last_portfolio_value = broker.portfolio_value

    @property
    def broker(self):
        return self.__broker

    @property
    def exchange(self):
        return self.__exchange

    def __iter__(self):
        return self

    def __next__(self):
        """
        Simulates one day of trading. Fetches data from the exchange and makes trading decisions
        via the broker. Records the portfolio status.
        """
        try:
            # Example market data fetching and decision-making logic
            trading_date, market_info, market_price = next(self.exchange)

            # Placeholder logic for trading decisions (buy/sell)
            # You need to implement your own decision logic here.
            # For example: self.broker.buy_option(...) or self.broker.sell_option(...)

            # Record portfolio value at the end of the day
            self.__results.append({
                'date': trading_date,
                'portfolio_value': self.broker.portfolio_value,
                'cash': self.broker.cash
            })

            # Update last portfolio value
            self.__last_portfolio_value = self.broker.portfolio_value

        except StopIteration:
            raise  # Rethrow StopIteration to signal the end of the iteration

        except Exception as e:
            print(f"Error on {self.exchange.curr_trading_time}: {e}")

    def run(self):
        """
        Runs the strategy over all trading days. Prints the status of the portfolio and cash at
        each step, and returns the results as a DataFrame.
        """
        for _ in self:
            print('-' * 40)
            print(f"PROCESSING DATE:  {self.exchange.curr_trading_time}")
            print(f"PORTFOLIO CASH {self.broker.cash}")
            print(f"PORTFOLIO VALUE {self.broker.portfolio_value}")
            print(f"PORTFOLIO positions {self.broker.positions}")

        print("Trading simulation complete.")

        # Convert results to DataFrame
        results_df = pd.DataFrame(self.__results)
        if not results_df.empty:
            results_df.set_index('date', inplace=True)

        return results_df
