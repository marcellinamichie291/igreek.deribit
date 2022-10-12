import numpy as np
import pandas as pd
from py_vollib_vectorized.implied_volatility import vectorized_implied_volatility
from py_lets_be_rational.exceptions import BelowIntrinsicException

def find_iv(price, spot, strike, maturity, r, flag, q):
    try:
        iv = vectorized_implied_volatility(price, spot, strike, maturity, r, flag, q)
    except (BelowIntrinsicException):
        pass
    return iv
