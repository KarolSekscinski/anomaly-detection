import pandas as pd
import time
import numpy as np


def simulate_btc_data(size=100, number_anomalies=0.05):
    # Generate normal BTC price data
    normal_data_prices = np.random.normal(loc=30000, scale=2000, size=(int(size * (1 - number_anomalies)), 1))
    anomaly_data_prices = np.random.uniform(low=20000, high=40000, size=(int(size * number_anomalies), 1))

    # Generate normal BTC volume data
    normal_volume_data = np.random.normal(loc=800, scale=200, size=(int(size * (1 - number_anomalies)), 1))
    anomaly_volume_data = np.random.uniform(low=10, high=1500, size=(int(size * number_anomalies), 1))

    # Combine normal and anomaly data for prices and volumes
    prices_data = np.vstack([normal_data_prices, anomaly_data_prices])
    volumes_data = np.vstack([normal_volume_data, anomaly_volume_data])

    # Shuffle data to mix anomalies and normal points
    combined_data = np.hstack([prices_data, volumes_data])  # Combine price and volume for consistent shuffling
    np.random.shuffle(combined_data)

    # Separate shuffled data back into prices and volumes
    prices_data = combined_data[:, 0]
    volumes_data = combined_data[:, 1]

    data = pd.DataFrame({
        "p": prices_data,
        "s": ["BTCUSDT" for _ in range(size)],
        "t": [time.time() for _ in range(size)],
        "v": volumes_data
    })

    return data
