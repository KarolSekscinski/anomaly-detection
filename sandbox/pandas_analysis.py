import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from sklearn.metrics import confusion_matrix
from datetime import datetime


def create_method_analysis(func, test_df, batch_size=0, title="Anomaly Detection"):
    # Fill NaN values in the dataframe
    test_df = test_df.fillna(0)

    # Print statistics on batch_size
    print(f"Mean batch size: {test_df['batch_size'].mean()}")
    print(f"Batch size STD: {test_df['batch_size'].std()}")
    print(f"Batch size Median: {test_df['batch_size'].median()}")

    # Plot histogram for batch_size
    plt.figure(figsize=(10, 6))
    plt.hist(test_df['batch_size'], color='blue', bins=120, alpha=0.5)
    plt.xlabel('Batch Size')
    plt.ylabel('Częstotliwość')
    plt.title('Histogram Batch Size')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.show()

    # Windowing the dataset
    if batch_size == 0:
        window_size = int(test_df["batch_size"].mean())
    else:
        window_size = int(batch_size)
    num_batches = len(test_df) // window_size + (1 if len(test_df) % window_size != 0 else 0)
    print(f"Number of batches: {num_batches}, batch size: {window_size}")

    # Processing data in batches and detecting anomalies
    batches = []
    for i in range(num_batches):
        start = i * window_size
        end = start + window_size
        batch_df = test_df.iloc[start:end]
        processed_batch = batch_df.copy()
        processed_batch["predicted_prices"] = func(batch_df["price"])
        processed_batch["predicted_volumes"] = func(batch_df["volume"])
        batches.append(processed_batch)

    batched_df = pd.concat(batches, ignore_index=True)

    # Compute confusion matrices for both price and volume anomalies
    y_true_prices = batched_df["price_pred"]
    y_pred_prices = batched_df["predicted_prices"].astype(int)
    y_true_volumes = batched_df["volume_pred"]
    y_pred_volumes = batched_df["predicted_volumes"].astype(int)

    conf_matrix_prices = confusion_matrix(y_true_prices, y_pred_prices)
    conf_matrix_volumes = confusion_matrix(y_true_volumes, y_pred_volumes)

    print("Confusion Matrix for Price Anomalies:")
    print(conf_matrix_prices)
    print("\nConfusion Matrix for Volume Anomalies:")
    print(conf_matrix_volumes)

    # Ensure proper conversion to datetime format
    batched_df["trade_ts"] = pd.to_datetime(batched_df["trade_ts"], format="%Y-%m-%d %H:%M:%S%z", errors='coerce')
    batched_df["ingestion_ts"] = pd.to_datetime(batched_df["ingestion_ts"], format="%Y-%m-%d %H:%M:%S%z",
                                                errors='coerce')

    # Calculate the time difference (time to ingest)
    batched_df["time_to_ingest"] = (batched_df["ingestion_ts"] - batched_df["trade_ts"]).dt.total_seconds()
    # Optional: Check if there are any invalid values due to coercion (NaT values)
    invalid_data = batched_df[batched_df["time_to_ingest"].isna()]
    if not invalid_data.empty:
        print("There are invalid timestamps, which have been coerced to NaT.")

    # Plot the time to ingest using a histogram
    plt.figure(figsize=(10, 6))
    plt.hist(batched_df["time_to_ingest"], bins=30)
    plt.title("Histogram czasów przetwarzania")
    plt.xlabel("Czas przetwarzania [s]")
    plt.ylabel("Częstotliwość")
    plt.grid(True)
    plt.show()

    # Mark anomalies based on price and volume predictions
    anomalies_df = batched_df[batched_df['price_pred'] == 1]
    pandas_df = batched_df[batched_df['predicted_prices'] == 1]

    # Price plot with anomalies
    y_min_value = batched_df["price"].min() * 0.9999
    y_max_value = batched_df["price"].max() * 1.0001
    plt.figure(figsize=(10, 6))
    plt.plot(batched_df["trade_ts"], batched_df["price"], label="Trade Price")
    plt.scatter(anomalies_df["trade_ts"], anomalies_df["price"], color='red', label="Spark", zorder=5)
    plt.scatter(pandas_df["trade_ts"], pandas_df["price"], color='green', label="Pandas", zorder=5)
    plt.title(f"Anomalie w kursie cen metodą {title}")
    plt.xlabel("Czas [s]")
    plt.ylabel("Cena [USD]")
    plt.xlim(batched_df["trade_ts"].min(), batched_df["trade_ts"].max())
    plt.ylim(y_min_value, y_max_value)
    plt.gca().xaxis.set_major_locator(mdates.MinuteLocator(interval=5))
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    plt.gca().xaxis.set_minor_locator(mdates.MinuteLocator(interval=1))
    plt.gca().xaxis.set_minor_formatter(mdates.DateFormatter('%H:%M'))
    plt.xticks(rotation=45)
    plt.legend()
    plt.grid(True)
    plt.show()

    # Volume plot with anomalies
    anomalies_df = batched_df[batched_df['volume_pred'] == 1]
    pandas_df = batched_df[batched_df['predicted_volumes'] == 1]
    y_min_value = 0
    y_max_value = batched_df["volume"].max() * 1.05
    plt.figure(figsize=(10, 6))
    plt.plot(batched_df["trade_ts"], batched_df["volume"], label="Trade Volume")
    plt.scatter(anomalies_df["trade_ts"], anomalies_df["volume"], color='red', label="Spark", zorder=5)
    plt.scatter(pandas_df["trade_ts"], pandas_df["volume"], color='green', label="Pandas", zorder=5)
    plt.title("Anomalie objętościowe")
    plt.xlabel("Czas [s]")
    plt.ylabel("Objętość")
    plt.xlim(batched_df["trade_ts"].min(), batched_df["trade_ts"].max())  # X-axis limits from min to max trade_ts
    plt.ylim(y_min_value, y_max_value)  # Y-axis limits with a margin
    plt.gca().xaxis.set_major_locator(mdates.MinuteLocator(interval=5))  # Set ticks every 5 minutes
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))  # Format time as hour:minute
    plt.gca().xaxis.set_minor_locator(mdates.MinuteLocator(interval=1))  # Set minor ticks every minute
    plt.gca().xaxis.set_minor_formatter(mdates.DateFormatter('%H:%M'))  # Format minor ticks
    plt.xticks(rotation=45)  # Rotate x-ticks for better readability
    plt.legend()
    plt.grid(True)
    plt.show()