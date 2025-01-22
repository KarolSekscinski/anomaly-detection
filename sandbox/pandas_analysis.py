import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from sklearn.metrics import confusion_matrix
import seaborn as sns
import numpy as np
from methods_from_spark import *


def batch_statistics_and_plots(df_z_score, df_iqr_score, df_arima_score, df_iforest_score, titles, x_label, y_label):
    n_rows = 2
    n_cols = 2
    fig, ax = plt.subplots(n_rows, n_cols, figsize=(12, 8))
    ax[0, 0].hist(df_z_score["batch_size"], color='blue', bins=120)
    ax[0, 0].set_title(f"{titles[0]}")
    ax[0, 0].set_xlabel(x_label)
    ax[0, 0].set_ylabel(y_label)
    ax[0, 1].hist(df_iqr_score["batch_size"], color='blue', bins=120)
    ax[0, 1].set_title(f"{titles[1]}")
    ax[0, 1].set_xlabel(x_label)
    ax[0, 1].set_ylabel(y_label)
    ax[1, 0].hist(df_arima_score["batch_size"], color='blue', bins=120)
    ax[1, 0].set_title(f"{titles[2]}")
    ax[1, 0].set_xlabel(x_label)
    ax[1, 0].set_ylabel(y_label)
    ax[1, 1].hist(df_iforest_score["batch_size"], color='blue', bins=120)
    ax[1, 1].set_title(f"{titles[3]}")
    ax[1, 1].set_xlabel(x_label)
    ax[1, 1].set_ylabel(y_label)
    plt.tight_layout()


def plot_ingestion_time_stats(df_z_score, df_iqr_score, df_arima_score, df_iforest_score, titles, x_label, y_label):
    timestamp_format = "%Y-%m-%d %H:%M:%S.%f%z"
    # Z-score
    df_z_score["trade_ts"] = pd.to_datetime(df_z_score["trade_ts"], format=timestamp_format, errors='coerce')
    df_z_score["ingestion_ts"] = pd.to_datetime(df_z_score["ingestion_ts"], format=timestamp_format, errors='coerce')
    df_z_score["time_to_ingest"] = (df_z_score["ingestion_ts"] - df_z_score["trade_ts"]).dt.total_seconds()
    # IQR
    df_iqr_score["trade_ts"] = pd.to_datetime(df_iqr_score["trade_ts"], format=timestamp_format, errors='coerce')
    df_iqr_score["ingestion_ts"] = pd.to_datetime(df_iqr_score["ingestion_ts"], format=timestamp_format,
                                                  errors='coerce')
    df_iqr_score["time_to_ingest"] = (df_iqr_score["ingestion_ts"] - df_iqr_score["trade_ts"]).dt.total_seconds()
    # Arima
    df_arima_score["trade_ts"] = pd.to_datetime(df_arima_score["trade_ts"], format=timestamp_format, errors='coerce')
    df_arima_score["ingestion_ts"] = pd.to_datetime(df_arima_score["ingestion_ts"], format=timestamp_format,
                                                    errors='coerce')
    df_arima_score["time_to_ingest"] = (df_arima_score["ingestion_ts"] - df_arima_score["trade_ts"]).dt.total_seconds()
    # iforest
    df_iforest_score["trade_ts"] = pd.to_datetime(df_iforest_score["trade_ts"], format=timestamp_format,
                                                  errors='coerce')
    df_iforest_score["ingestion_ts"] = pd.to_datetime(df_iforest_score["ingestion_ts"], format=timestamp_format,
                                                      errors='coerce')
    df_iforest_score["time_to_ingest"] = (
                df_iforest_score["ingestion_ts"] - df_iforest_score["trade_ts"]).dt.total_seconds()

    invalid_data = df_z_score[df_z_score["time_to_ingest"].isna()]
    invalid_trade_ts = df_z_score[df_z_score["trade_ts"].isna()]
    invalid_ingestion_ts = df_z_score[df_z_score["ingestion_ts"].isna()]
    if not invalid_trade_ts.empty or not invalid_ingestion_ts.empty:
        print("Unparsed rows in trade_ts or ingestion_ts")
        print("Invalid trade_ts:", invalid_trade_ts)
        print("Invalid ingestion_ts:", invalid_ingestion_ts)
    invalid_data = df_iqr_score[df_iqr_score["time_to_ingest"].isna()]
    if not invalid_data.empty:
        print("There are invalid timestamps in df_iqr_score, which have been coerced to NaT.")
    invalid_data = df_arima_score[df_arima_score["time_to_ingest"].isna()]
    if not invalid_data.empty:
        print("There are invalid timestamps in df_arima_score, which have been coerced to NaT.")
    invalid_data = df_iforest_score[df_iforest_score["time_to_ingest"].isna()]
    if not invalid_data.empty:
        print("There are invalid timestamps in df_iforest_score, which have been coerced to NaT.")

    n_rows = 2
    n_cols = 2
    fig, ax = plt.subplots(n_rows, n_cols, figsize=(12, 8))
    plt.title("Czasy przetwarzania wybranych algorytmów")
    ax[0, 0].hist(df_z_score["time_to_ingest"], color='blue', bins=30)
    ax[0, 0].set_title(f"{titles[0]}")
    ax[0, 0].set_xlabel(x_label)
    ax[0, 0].set_ylabel(y_label)
    ax[0, 1].hist(df_iqr_score["time_to_ingest"], color='blue', bins=30)
    ax[0, 1].set_title(f"{titles[1]}")
    ax[0, 1].set_xlabel(x_label)
    ax[0, 1].set_ylabel(y_label)
    ax[1, 0].hist(df_arima_score["time_to_ingest"], color='blue', bins=30)
    ax[1, 0].set_title(f"{titles[2]}")
    ax[1, 0].set_xlabel(x_label)
    ax[1, 0].set_ylabel(y_label)
    ax[1, 1].hist(df_iforest_score["time_to_ingest"], color='blue', bins=30)
    ax[1, 1].set_title(f"{titles[3]}")
    ax[1, 1].set_xlabel(x_label)
    ax[1, 1].set_ylabel(y_label)
    plt.tight_layout()


def calculate_anomalies(batch_size, test_df, func, iforest):
    if not iforest:
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
    else:
        test_df["predicted_prices"] = func(test_df["price"])
        test_df["predicted_volumes"] = func(test_df["volume"])
        batched_df = test_df.copy()

    batched_df = batched_df.fillna(0)
    return batched_df


def calculate_conf_mat(batch_size, test_df, func, to_analyze, iforest=False):
    if iforest:
        batched_df = calculate_anomalies(batch_size, test_df, func, iforest)
    else:
        batched_df = calculate_anomalies(batch_size, test_df, func, iforest)
    y_true_prices = batched_df["price_pred"]  # predicted in spark
    y_pred_prices = batched_df["predicted_prices"].astype(int)  # predicted in pandas
    y_true_volumes = batched_df["volume_pred"]  # predicted in spark
    y_pred_volumes = batched_df["predicted_volumes"].astype(int)  # predicted in pandas
    group_names = ["True Neg", "False Pos", "False Neg", "True Pos"]
    if to_analyze == "price":
        conf_matrix_prices = confusion_matrix(y_pred_prices, y_true_prices)
        group_counts = ["{0:0.0f}".format(value) for value in conf_matrix_prices.flatten()]
        group_percentages = ["{0:.2%}".format(value) for value in
                             conf_matrix_prices.flatten() / np.sum(conf_matrix_prices)]
        labels = [f"{v1}\n{v2}\n{v3}" for v1, v2, v3 in zip(group_names, group_counts, group_percentages)]
        labels = np.asarray(labels).reshape(2, 2)
        return conf_matrix_prices, labels
    else:
        conf_matrix_volumes = confusion_matrix(y_pred_volumes, y_true_volumes)
        group_counts = ["{0:0.0f}".format(value) for value in conf_matrix_volumes.flatten()]
        group_percentages = ["{0:.2%}".format(value) for value in
                             conf_matrix_volumes.flatten() / np.sum(conf_matrix_volumes)]
        labels = [f"{v1}\n{v2}\n{v3}" for v1, v2, v3 in zip(group_names, group_counts, group_percentages)]
        labels = np.asarray(labels).reshape(2, 2)
        return conf_matrix_volumes, labels


def plot_confusion_matrices_separately(df_z_score, df_iqr_score, df_arima_score, df_iforest_score, titles, raw=False):
    def plot_single_conf_matrix(df, anomalies, metric, title, ax):
        """
        Helper function to calculate and plot a single confusion matrix.
        """
        df = df.fillna(0)
        conf_matrix, labels = calculate_conf_mat(batch_size, df, anomalies, metric)
        sns.heatmap(conf_matrix, annot=labels, fmt='', cmap="Blues", ax=ax)
        ax.set_title(title)

    if raw:
        batch_size = 15
    else:
        batch_size = 0

    dfs_and_titles = [
        (df_z_score, z_score_anomalies, titles[0]),
        (df_iqr_score, iqr_anomalies, titles[1]),
        (df_arima_score, arima_anomalies, titles[2]),
        (df_iforest_score, iforest_anomalies, titles[3]),
    ]

    for i, (df, anomalies, title) in enumerate(dfs_and_titles):
        fig, ax = plt.subplots(1, 2, figsize=(12, 5))  # One row, two columns for price and volume
        plot_single_conf_matrix(df, anomalies, "price", f"Ceny {title}", ax[0])
        plot_single_conf_matrix(df, anomalies, "volume", f"Wolumen {title}", ax[1])
        plt.tight_layout()
        plt.show()



def create_method_analysis(func, test_df, batch_size=0, title="Anomaly Detection", iforest=False):
    # Fill NaN values in the dataframe
    test_df = test_df.fillna(0)

    # # Print statistics on batch_size
    # print(f"Mean batch size: {test_df['batch_size'].mean()}")
    # print(f"Batch size STD: {test_df['batch_size'].std()}")
    # print(f"Batch size Median: {test_df['batch_size'].median()}")
    #
    # # Plot histogram for batch_size
    # plt.figure(figsize=(10, 6))
    # plt.hist(test_df['batch_size'], color='blue', bins=120, alpha=0.5)
    # plt.xlabel('wielkość partii')
    # plt.ylabel('Częstotliwość')
    # plt.title('Histogram wielkości partii')
    # plt.xticks(rotation=45, ha='right')
    # plt.tight_layout()
    # plt.show()

    if iforest:
        test_df["predicted_prices"] = func(test_df["price"])
        test_df["predicted_volumes"] = func(test_df["volume"])
        test_df = test_df.fillna(0)
        batched_df = test_df.copy()
    else:
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
        batched_df = batched_df.fillna(0)
    # Compute confusion matrices for both price and volume anomalies
    y_true_prices = batched_df["price_pred"]
    y_pred_prices = batched_df["predicted_prices"].astype(int)
    y_true_volumes = batched_df["volume_pred"]
    y_pred_volumes = batched_df["predicted_volumes"].astype(int)

    conf_matrix_prices = confusion_matrix(y_true_prices, y_pred_prices)
    conf_matrix_volumes = confusion_matrix(y_true_volumes, y_pred_volumes)

    group_names = ["True Neg", "False Pos", "False Neg", "True Pos"]
    group_counts = ["{0:0.0f}".format(value) for value in conf_matrix_prices.flatten()]

    # print("Confusion Matrix for Price Anomalies:")
    # fig, ax = plt.subplots(1, 2, figsize=(8, 8))
    # sns.heatmap(conf_matrix_prices/np.sum(conf_matrix_prices), annot=True, fmt='.2%', cmap="Blues", ax=ax[0])
    # print("Confusion Matrix for Volume Anomalies:")
    # sns.heatmap(conf_matrix_volumes / np.sum(conf_matrix_volumes), annot=True, fmt='.2%', cmap="Blues", ax=ax[1])
    # # print(conf_matrix_volumes)

    # Ensure proper conversion to datetime format
    ts_format = "%Y-%m-%d %H:%M:%S.%f%z"
    batched_df["trade_ts"] = pd.to_datetime(batched_df["trade_ts"], format=ts_format, errors='coerce')
    batched_df["ingestion_ts"] = pd.to_datetime(batched_df["ingestion_ts"], format=ts_format,
                                                errors='coerce')

    # Calculate the time difference (time to ingest)
    batched_df["time_to_ingest"] = (batched_df["ingestion_ts"] - batched_df["trade_ts"]).dt.total_seconds()
    # Optional: Check if there are any invalid values due to coercion (NaT values)
    invalid_data = batched_df[batched_df["time_to_ingest"].isna()]
    if not invalid_data.empty:
        print("There are invalid timestamps, which have been coerced to NaT.")

    # # Plot the time to ingest using a histogram
    # plt.figure(figsize=(10, 6))
    # plt.hist(batched_df["time_to_ingest"], bins=30)
    # plt.title("Histogram czasów przetwarzania")
    # plt.xlabel("Czas przetwarzania [s]")
    # plt.ylabel("Częstotliwość")
    # plt.grid(True)
    # plt.show()

    # Define y-axis limits for price
    y_min_price = batched_df["price"].min() * 0.9999
    y_max_price = batched_df["price"].max() * 1.0001

    # Define y-axis limits for volume
    y_min_volume = 0
    y_max_volume = batched_df["volume"].max() * 1.05

    # Create subplots
    fig, ax = plt.subplots(2, 1, figsize=(12, 10))

    # Plot Price anomalies
    anomalies_price = batched_df[batched_df['price_pred'] == 1]
    pandas_price = batched_df[batched_df['predicted_prices'] == 1]

    ax[0].plot(batched_df["trade_ts"], batched_df["price"], label="Kurs cen")
    ax[0].scatter(anomalies_price["trade_ts"], anomalies_price["price"], color='red', label="Spark", zorder=5)
    ax[0].scatter(pandas_price["trade_ts"], pandas_price["price"], color='green', label="Pandas", zorder=5, alpha=0.5)
    ax[0].set_title("Anomalie w kursie cen")
    ax[0].set_xlabel("Czas [s]")
    ax[0].set_ylabel("Cena [USD]")
    ax[0].set_xlim(batched_df["trade_ts"].min(), batched_df["trade_ts"].max())
    ax[0].set_ylim(y_min_price, y_max_price)
    ax[0].xaxis.set_major_locator(mdates.MinuteLocator(interval=5))
    ax[0].xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    ax[0].xaxis.set_minor_locator(mdates.MinuteLocator(interval=1))
    ax[0].xaxis.set_minor_formatter(mdates.DateFormatter('%H:%M'))
    ax[0].tick_params(axis='x', rotation=45)
    ax[0].legend()
    ax[0].grid(True)

    # Plot Volume anomalies
    anomalies_volume = batched_df[batched_df['volume_pred'] == 1]
    pandas_volume = batched_df[batched_df['predicted_volumes'] == 1]

    ax[1].plot(batched_df["trade_ts"], batched_df["volume"], label="Wolumen")
    ax[1].scatter(anomalies_volume["trade_ts"], anomalies_volume["volume"], color='red', label="Spark",
                  zorder=5)
    ax[1].scatter(pandas_volume["trade_ts"], pandas_volume["volume"], color='green', label="Pandas", zorder=5, alpha=0.5)
    ax[1].set_title("Anomalie objętościowe")
    ax[1].set_xlabel("Czas [s]")
    ax[1].set_ylabel("Objętość")
    ax[1].set_xlim(batched_df["trade_ts"].min(), batched_df["trade_ts"].max())
    ax[1].set_ylim(y_min_volume, y_max_volume)
    ax[1].xaxis.set_major_locator(mdates.MinuteLocator(interval=5))
    ax[1].xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    ax[1].xaxis.set_minor_locator(mdates.MinuteLocator(interval=1))
    ax[1].xaxis.set_minor_formatter(mdates.DateFormatter('%H:%M'))
    ax[1].tick_params(axis='x', rotation=45)
    ax[1].legend()
    ax[1].grid(True)

    plt.tight_layout()
    plt.show()