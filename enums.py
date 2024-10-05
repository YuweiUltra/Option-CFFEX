from enum import Enum


class ExchangeTypes(Enum):
    Option = 1


class AssetTypes(Enum):
    Option = 1
    Future = 2


class OrderTypes(Enum):
    Open = 0
    Filled = 1
    PartiallyFilled = 2
    Cancelled = 3
