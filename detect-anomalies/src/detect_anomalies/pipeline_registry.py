"""Project pipelines."""
from __future__ import annotations

from kedro.pipeline import Pipeline
from detect_anomalies.pipelines.anomaly_detection import pipeline as anomaly_detection_pipeline

def register_pipelines() -> dict[str, Pipeline]:
    """Register the project's pipelines.

    Returns:
        A mapping from pipeline names to ``Pipeline`` objects.
    """
    anomaly_pipeline = anomaly_detection_pipeline.create_pipeline()
    pipelines = {
        "anomaly_detection": anomaly_pipeline,
        "__default__": anomaly_pipeline  # Set this to default if you want to run it without specifying a name
    }
    return pipelines
