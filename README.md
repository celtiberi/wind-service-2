# Wind Data API

This API provides wind data and visualizations for specified geographical regions using GFS (Global Forecast System) data.

## Setup

1. Create a virtual environment and activate it:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Place your GFS GRIB file in the `gribs` directory:
- The file should be named `gfs.t12z.pgrb2.0p25.f000.grib2`
- Or update the filename in `app/services/wind_service.py`

4. Run the API:
```bash
uvicorn app.main:app --reload
```

The API will be available at `http://localhost:8000`

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
    "image_base64": "base64_encoded_png_image"
}
```

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
├── gribs/
│   └── gfs.t12z.pgrb2.0p25.f000.grib2
├── tests/
│   ├── __init__.py
│   ├── conftest.py
│   └── test_wind_api.py
├── requirements.txt
└── pyproject.toml
```


# With curl
curl -X POST "http://localhost:8000/wind-data" \
  -H "Content-Type: application/json" \
  -d '{
    "min_lat": 9.252,
    "max_lat": 22.328,
    "min_lon": -87.537,
    "max_lon": -66.356
  }' | jq -r '.image_base64' | base64 -d > wind_map.png