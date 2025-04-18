# Weather Data API

This API provides comprehensive weather data and visualizations for specified geographical regions using GFS (Global Forecast System) data.

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

The API will be available at `http://localhost:8000`. The service automatically processes GRIB files from NOAA's GFS system. When data is not yet available, the API will return a 503 status code.

## API Endpoints

### POST /wind-data
Get wind data and visualization for a specified region. You can specify the region either by name, point coordinates, or bounding box coordinates.

Request body:
```json
{
    "name": "Caribbean Sea"
}
```
OR
```json
{
    "lat": 20.090,
    "lon": -83.502
}
```
OR
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
        }
    ],
    "image_base64": "base64_encoded_png_image",
    "grib_file": {
        "path": "gfs.t12z.pgrb2.0p25.f000",
        "download_time": "2024-03-22T12:30:00",
        "metadata": {
            "cycle": "t12z",
            "resolution": "0p25",
            "forecast_hour": "f000"
        }
    },
    "description": "Wind conditions description"
}
```

### POST /wave-data
Get wave data and visualization for a specified region. Supports the same region specification methods as /wind-data.

Request body:
```json
{
    "name": "Caribbean Sea"
}
```
OR
```json
{
    "lat": 20.090,
    "lon": -83.502
}
```
OR
```json
{
    "min_lat": 37.5,
    "max_lat": 42.5,
    "min_lon": -72.5,
    "max_lon": -67.5
}
```

Response includes wave height, period, and direction data:
```json
{
    "valid_time": "2024-03-22T12:00:00",
    "data_points": [
        {
            "latitude": 10.0,
            "longitude": -65.0,
            "wave_height": 1.5,
            "wave_period_s": 8.2,
            "wave_direction_deg": 45.0
        }
    ],
    "image_base64": "base64_encoded_png_image",
    "grib_file": {
        "path": "gfswave.t12z.global.0p16.f000.grib2",
        "download_time": "2024-03-22T12:30:00",
        "metadata": {
            "cycle": "t12z",
            "resolution": "0p16",
            "forecast_hour": "f000"
        }
    },
    "description": "Wave conditions description"
}
```

### POST /marine-hazards
Get comprehensive marine hazards data including wind, waves, and storm indicators for a region. Supports the same region specification methods as /wind-data.

Request body:
```json
{
    "name": "Caribbean Sea"
}
```
OR
```json
{
    "lat": 20.090,
    "lon": -83.502
}
```
OR
```json
{
    "min_lat": 37.5,
    "max_lat": 42.5,
    "min_lon": -72.5,
    "max_lon": -67.5
}
```

Response includes combined hazard data and indicators:
```json
{
    "valid_time": "2024-03-22T12:00:00",
    "data_points": [...],
    "image_base64": "base64_encoded_png_image",
    "grib_file": {...},
    "storm_indicators": [...],
    "description": "Marine hazards description"
}
```

### POST /marine-forecast
Get marine forecast for a specific location or area. You can specify the location either by name, point coordinates, or bounding box coordinates.

Request body:
```json
{
    "name": "Caribbean Sea"
}
```
OR
```json
{
    "lat": 20.090,
    "lon": -83.502
}
```
OR
```json
{
    "min_lat": 9.252,
    "max_lat": 22.328,
    "min_lon": -87.537,
    "max_lon": -66.356
}
```

Response:
```json
{
    "forecast": "Marine forecast text...",
    "zone_id": "AMZ046",
    "lat": 20.090,
    "lon": -83.502,
    "error": null
}
```

### GET /health
Health check endpoint returning service status.

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
│   ├── services/
│   │   ├── weather_service.py
│   │   ├── process_weather_data.py
│   │   └── noaa_marine_forecast.py
│   └── natural_earth/
│       ├── ne_10m_geography_marine_polys.shp
│       ├── ne_10m_admin_0_countries.shp
│       └── ne_10m_lakes.shp
├── tests/
│   ├── __init__.py
│   ├── conftest.py
│   └── test_weather_api.py
├── requirements.txt
└── pyproject.toml
```

## Example Usage

Using curl to get wind data for the Caribbean Sea:
```bash
curl -X POST "http://localhost:8000/wind-data" \
     -H "Content-Type: application/json" \
     -d '{"name": "Caribbean Sea"}' | jq -r '.image_base64' | base64 -d > wind_map.png
```

Get wave data for the Caribbean Sea:
```bash
curl -X POST "http://localhost:8000/wave-data" \
     -H "Content-Type: application/json" \
     -d '{"name": "Caribbean Sea"}' | jq -r '.image_base64' | base64 -d > wave_map.png
```

Get marine hazards data:
```bash
curl -X POST "http://localhost:8000/marine-hazards" \
     -H "Content-Type: application/json" \
     -d '{"name": "Caribbean Sea"}' | jq -r '.image_base64' | base64 -d > hazards_map.png
```

Get marine forecast by name:
```bash
curl -X POST "http://localhost:8000/marine-forecast" \
     -H "Content-Type: application/json" \
     -d '{"name": "Caribbean Sea"}'
```

Get marine forecast by point:
```bash
curl -X POST "http://localhost:8000/marine-forecast" \
     -H "Content-Type: application/json" \
     -d '{"lat": 20.090, "lon": -83.502}'
```

```bash
curl -X POST "http://localhost:8000/marine-forecast" \
     -H "Content-Type: application/json" \
     -d '{"lat": 42.391, "lon": -30.366}'
```


Get marine forecast by bounding box:
```bash
curl -X POST "http://localhost:8000/marine-forecast" \
     -H "Content-Type: application/json" \
     -d '{
       "min_lat": 9.252,
       "max_lat": 22.328,
       "min_lon": -87.537,
       "max_lon": -66.356
     }'
```

Using coordinates (5°x5° box in the Caribbean):
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

Get wind data by point:
```bash
curl -X POST "http://localhost:8000/wind-data" \
     -H "Content-Type: application/json" \
     -d '{"lat": 20.090, "lon": -83.502}' | jq -r '.image_base64' | base64 -d > wind_map.png
```

```bash
curl -X POST "http://localhost:8000/wind-data" \
     -H "Content-Type: application/json" \
     -d '{"lat": 42.391, "lon": -30.366}' | jq -r '.image_base64' | base64 -d > wind_map.png
```

Get wave data by point:
```bash
curl -X POST "http://localhost:8000/wave-data" \
     -H "Content-Type: application/json" \
     -d '{"lat": 20.090, "lon": -83.502}' | jq -r '.image_base64' | base64 -d > wave_map.png
```

Get marine hazards by point:
```bash
curl -X POST "http://localhost:8000/marine-hazards" \
     -H "Content-Type: application/json" \
     -d '{"lat": 20.090, "lon": -83.502}'
```
