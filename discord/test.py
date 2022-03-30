from pairs import Pairs

weights = {"ETH":1, "SOL":-1}
lookback = 30
p = Pairs()
data, pct_ret, text = p.index(weights, lookback_window=lookback)
p.delete_data()