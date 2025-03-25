# Wind Data API

This API provides wind data and visualizations for specified geographical regions using GFS (Global Forecast System) data.

## Setup

1. Create and activate a conda environment:
```bash
conda create -n weather-service python=3.12
conda activate weather-service
```

2. Install dependencies:
```bash
conda install --file requirements.txt
```

3. Run the API:
```bash
uvicorn app.main:app --reload
```

The API will be available at `http://localhost:8000`. The service automatically downloads and updates GRIB files from NOAA's GFS system. When new data is not yet available, the API will return a 503 status code.

## API Endpoints

### POST /wind-data
Get wind data and visualization for a specified bounding box.

Request body:
```json
{
    "min_lat": 37.5,
    "max_lat": 42.5,
    "min_lon": -72.5,
    "max_lon": -67.5
}
```

Response:
```json
{
    "valid_time": "2024-03-22T12:00:00",
    "data_points": [
        {
            "latitude": 37.5,
            "longitude": -72.5,
            "wind_speed_knots": 15.2
        },
        ...
    ],
    "image_base64": "base64_encoded_png_image",
    "grib_file": {
        "filename": "gfs.t12z.pgrb2.0p25.f000",
        "cycle_time": "12z",
        "download_time": "2024-03-22T12:30:00",
        "forecast_hour": 0
    }
}
```

The response includes:
- `valid_time`: When the forecast is valid for
- `data_points`: Array of wind data points in the requested region
- `image_base64`: PNG visualization of wind speed and direction using wind barbs
- `grib_file`: Information about the GRIB file used for the data

If the GRIB files are not ready, the API will return a 503 Service Unavailable status.

### GET /health
Health check endpoint.

## API Documentation

Once the API is running, you can access:
- Interactive API docs (Swagger UI): `http://localhost:8000/docs`
- Alternative API docs (ReDoc): `http://localhost:8000/redoc`

## Project Structure
```
weather-service/
├── app/
│   ├── __init__.py
│   ├── main.py
│   ├── models/
│   │   └── schemas.py
│   └── services/
│       └── wind_service.py
├── gfs_atmos_p25/
│   └── gfs.t12z.pgrb2.0p25.f000
├── tests/
│   ├── __init__.py
│   ├── conftest.py
│   └── test_wind_api.py
├── requirements.txt
└── pyproject.toml
```

## Example Usage

Using curl to save the wind map visualization:
```bash
curl -X POST "http://localhost:8000/wind-data" \
  -H "Content-Type: application/json" \
  -d '{
    "min_lat": 9.252,
    "max_lat": 22.328,
    "min_lon": -87.537,
    "max_lon": -66.356
  }' | jq -r '.image_base64' | base64 -d > wind_map.png
```

```bash
curl -X POST "http://localhost:8000/wind-data" \
     -H "Content-Type: application/json" \
     -d '{"name": "Caribbean Sea"}' | jq -r '.image_base64' | base64 -d > wind_map.png
```