"""API clients for SDLC data sources.

Each client implements the BaseAPIClient protocol: accepts an Airflow
Connection and endpoint config, provides an extract() method that yields
dicts of raw data with pagination handled internally.
"""
