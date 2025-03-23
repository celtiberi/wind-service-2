import pytest
import base64
from PIL import Image
from io import BytesIO
from fastapi.testclient import TestClient
from app.main import app
import matplotlib
matplotlib.use('Agg')  # Set the backend to non-interactive 'Agg'

# Create test client
client = TestClient(app)

def test_wind_data_endpoint():
    """Test the wind-data endpoint and display results"""
    # Test data - 5°x5° box around New York area
    test_data = {
        "min_lat": 37.5,
        "max_lat": 42.5,
        "min_lon": -72.5,
        "max_lon": -67.5
    }

    # Make the API request
    response = client.post("/wind-data", json=test_data)
    
    # Assert response status code
    assert response.status_code == 200, f"API request failed with status code {response.status_code}"
    
    # Parse response data
    data = response.json()
    
    # Print response data
    print("\nAPI Response:")
    print(f"Valid Time: {data['valid_time']}")
    print(f"Number of data points: {len(data['data_points'])}")
    
    # Print first few data points
    print("\nFirst 5 data points:")
    for point in data['data_points'][:5]:
        print(f"Lat: {point['latitude']:.2f}°N, Lon: {point['longitude']:.2f}°W, "
              f"Wind Speed: {point['wind_speed_knots']:.1f} knots")
    
    # Save the image to a file instead of displaying it
    try:
        # Decode base64 image
        image_data = base64.b64decode(data['image_base64'])
        image = Image.open(BytesIO(image_data))
        
        # Save the image to a file
        output_path = "test_wind_map.png"
        image.save(output_path)
        print(f"\nWind map saved to: {output_path}")
        
    except Exception as e:
        print(f"Error saving image: {e}")
    
    # Additional assertions
    assert isinstance(data['valid_time'], str), "valid_time should be a string"
    assert isinstance(data['data_points'], list), "data_points should be a list"
    assert len(data['data_points']) > 0, "data_points should not be empty"
    assert isinstance(data['image_base64'], str), "image_base64 should be a string"
    
    # Validate data point structure
    for point in data['data_points']:
        assert 'latitude' in point, "Each data point should have latitude"
        assert 'longitude' in point, "Each data point should have longitude"
        assert 'wind_speed_knots' in point, "Each data point should have wind_speed_knots"
        assert isinstance(point['latitude'], (int, float)), "latitude should be numeric"
        assert isinstance(point['longitude'], (int, float)), "longitude should be numeric"
        assert isinstance(point['wind_speed_knots'], (int, float)), "wind_speed_knots should be numeric"

def test_health_endpoint():
    """Test the health check endpoint"""
    response = client.get("/health")
    assert response.status_code == 200, f"Health check failed with status code {response.status_code}"
    assert response.json() == {"status": "healthy"}, "Health check response should indicate healthy status" 