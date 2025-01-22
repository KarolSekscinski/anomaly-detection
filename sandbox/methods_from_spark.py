import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
from statsmodels.tsa.arima.model import ARIMA
import warnings


def z_score_anomalies(column: pd.Series) -> pd.Series:
    """
    Vectorized Z-score anomaly detection.
    Flags values with a Z-score greater than 3 or less than -3 as anomalies.
    """
    z_scores = (column - column.mean()) / column.std()
    return (z_scores > 3) | (z_scores < -3)


def iqr_anomalies(column: pd.Series) -> pd.Series:
    """
    Vectorized IQR-based anomaly detection.
    Flags values outside the range [Q1 - 1.5*IQR, Q3 + 1.5*IQR].
    """
    q1 = column.quantile(0.25)
    q3 = column.quantile(0.75)
    iqr = q3 - q1
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr
    return (column < lower_bound) | (column > upper_bound)


def arima_anomalies(column: pd.Series) -> pd.Series:
    """
    Vectorized ARIMA anomaly detection.
    Fits an ARIMA model and flags residuals exceeding a threshold.
    """
    try:
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore")
            model = ARIMA(column, order=(1, 1, 1))
            model_fit = model.fit()
        forecast = model_fit.fittedvalues
        residuals = column - forecast
        threshold = 3 * residuals.std()  # Anomalies if residuals exceed 3 std deviations
        return residuals.abs() > threshold
    except:
        # Return all False if ARIMA fitting fails
        return pd.Series([False] * len(column))


def iforest_anomalies(column: pd.Series) -> pd.Series:
    """
    Vectorized Isolation Forest anomaly detection.
    Fits an Isolation Forest model and flags anomalies (-1).
    """
    column_reshaped = column.values.reshape(-1, 1)
    model = IsolationForest(contamination=0.05)  # Adjust contamination as needed
    predictions = model.fit_predict(column_reshaped)
    return pd.Series(predictions == -1)  # -1 indicates anomalies