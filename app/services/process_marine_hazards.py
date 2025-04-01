import pygrib
import numpy as np
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import base64
from io import BytesIO
from typing import Tuple, List, Dict, Optional
from datetime import datetime
from .process_weather_data import ProcessWeatherData, logger
import matplotlib.lines as mlines
from app.models.schemas import GribFile

class ProcessMarineHazards(ProcessWeatherData):
    def process_data(self, min_lat: float, max_lat: float, min_lon: float, max_lon: float) -> Tuple[List[dict], str, datetime, GribFile, Optional[Dict], str]:
        logger.info(f"Processing marine hazards for bounding box: ({min_lat}, {max_lat}, {min_lon}, {max_lon})")
        
        if not self._wave_grib or not self._atmos_grib_file_data:
            raise ValueError("Atmospheric GRIB file not available")

        # Extract wind gusts
        try:
            gust_grb = self._wave_grib.select(name='Wind speed (gust)')[0]
            gust_data_full, lats_full, lons_full = gust_grb.data()
            wind_speed_knots, lats, lons = self._slice_data_to_bounding_box(gust_data_full, lats_full, lons_full, min_lat, max_lat, min_lon, max_lon)
            wind_speed_knots = wind_speed_knots * 1.94384  # Convert m/s to knots
            max_wind_speed = np.nanmax(wind_speed_knots)
            logger.debug(f"Wind gusts (knots) range: {wind_speed_knots.min()} to {max_wind_speed}")
        except Exception as e:
            logger.error(f"Error extracting wind gusts: {e}", exc_info=True)
            raise Exception(f"Error extracting wind gusts: {e}")

        # Create data points list (wind speed at each grid point)
        data_points = []
        try:
            for i in range(wind_speed_knots.shape[0]):
                for j in range(wind_speed_knots.shape[1]):
                    data_points.append({
                        'latitude': float(lats[i, j]),
                        'longitude': float(lons[i, j]),
                        'wind_speed_knots': float(wind_speed_knots[i, j])
                    })
            logger.info(f"Created {len(data_points)} wind data points")
        except Exception as e:
            logger.error(f"Error creating wind data points: {e}", exc_info=True)
            raise Exception(f"Error creating wind data points: {e}")

        # Process storm and additional hazard indicators
        try:
            hazard_indicators = self._process_hazard_indicators(min_lat, max_lat, min_lon, max_lon, lats_full, lons_full)
            logger.info("Hazard indicators processed successfully")
        except Exception as e:
            logger.error(f"Error processing hazard indicators: {e}", exc_info=True)
            raise Exception(f"Error processing hazard indicators: {e}")

        # Generate and encode the plot
        try:
            image_base64 = self._generate_plot(lats, lons, wind_speed_knots, min_lat, max_lat, min_lon, max_lon, 
                                              gust_grb.validDate, self._atmos_grib_file_data, hazard_indicators=hazard_indicators)
            logger.info("Marine hazards plot generated successfully")
        except Exception as e:
            logger.error(f"Error generating marine hazards plot: {e}", exc_info=True)
            raise Exception(f"Error generating marine hazards plot: {e}")

        # Generate text description
        description = self._generate_description(hazard_indicators, max_wind_speed, min_lat, max_lat, min_lon, max_lon)

        # Remove spatial_data to avoid serialization issues
        if hazard_indicators and "spatial_data" in hazard_indicators:
            logger.debug("Removing spatial_data from hazard_indicators")
            del hazard_indicators["spatial_data"]

        return data_points, image_base64, gust_grb.validDate, self._atmos_grib_file_data, hazard_indicators, description

    def _process_hazard_indicators(self, min_lat: float, max_lat: float, min_lon: float, max_lon: float, 
                                   lats_full: np.ndarray, lons_full: np.ndarray) -> Dict:
        logger.info(f"Processing hazard indicators for bounding box: ({min_lat}, {max_lat}, {min_lon}, {max_lon})")
        indicators = {
            "storm_potential": False,
            "severe_storm_risk": False,
            "low_visibility": False,
            "icing_risk": False,
            "cold_risk": False,
            "heat_risk": False,
            "fog_risk": False,
            "details": {},
            "spatial_data": {}
        }

        # Storm Potential (Heavy Rain + Instability + Reflectivity)
        try:
            precip_grb = self._wave_grib.select(name='Precipitation rate')[0]
            precip_data_full, _, _ = precip_grb.data()
            precip_data, lats, lons = self._slice_data_to_bounding_box(precip_data_full, lats_full, lons_full, min_lat, max_lat, min_lon, max_lon)
            precip_rate_mmh = precip_data * 3600  # kg m^-2 s^-1 to mm/h
            max_precip_rate = np.nanmax(precip_rate_mmh)
            indicators["details"]["max_precipitation_rate_mmh"] = float(max_precip_rate)

            cape_grb = self._wave_grib.select(name='Convective available potential energy', typeOfLevel='pressureFromGroundLayer', level=18000)[0]
            cape_data_full, _, _ = cape_grb.data()
            cape_data, _, _ = self._slice_data_to_bounding_box(cape_data_full, lats_full, lons_full, min_lat, max_lat, min_lon, max_lon)
            max_cape = np.nanmax(cape_data)
            indicators["details"]["max_cape_jkg"] = float(max_cape)

            reflectivity_grb = self._wave_grib.select(name='Maximum/Composite radar reflectivity')[0]
            reflectivity_data_full, _, _ = reflectivity_grb.data()
            reflectivity_data, _, _ = self._slice_data_to_bounding_box(reflectivity_data_full, lats_full, lons_full, min_lat, max_lat, min_lon, max_lon)
            max_reflectivity = np.nanmax(reflectivity_data)
            indicators["details"]["max_reflectivity_db"] = float(max_reflectivity)

            storm_mask = (precip_rate_mmh > 5) | (cape_data > 1000) | (reflectivity_data > 40)
            if np.any(storm_mask):
                indicators["storm_potential"] = True
            indicators["spatial_data"]["storm_mask"] = storm_mask
            indicators["spatial_data"]["lats"] = lats
            indicators["spatial_data"]["lons"] = lons
        except Exception as e:
            logger.error(f"Error processing storm potential: {e}", exc_info=True)

        # Severe Storm Risk (CAPE + Reflectivity)
        try:
            severe_mask = (cape_data > 1500) & (reflectivity_data > 50)
            if np.any(severe_mask):
                indicators["severe_storm_risk"] = True
            indicators["spatial_data"]["severe_mask"] = severe_mask
        except Exception as e:
            logger.error(f"Error processing severe storm risk: {e}", exc_info=True)

        # Low Visibility
        try:
            vis_grb = self._wave_grib.select(name='Visibility')[0]
            vis_data_full, _, _ = vis_grb.data()
            vis_data, _, _ = self._slice_data_to_bounding_box(vis_data_full, lats_full, lons_full, min_lat, max_lat, min_lon, max_lon)
            vis_nm = vis_data / 1852  # Convert m to nautical miles
            low_vis_mask = vis_nm < 1
            if np.any(low_vis_mask):
                indicators["low_visibility"] = True
            indicators["spatial_data"]["low_vis_mask"] = low_vis_mask
            indicators["details"]["min_visibility_nm"] = float(np.nanmin(vis_nm))
        except Exception as e:
            logger.error(f"Error processing visibility: {e}", exc_info=True)

        # Icing Risk
        try:
            frozen_grb = self._wave_grib.select(name='Percent frozen precipitation')[0]
            frozen_data_full, _, _ = frozen_grb.data()
            frozen_data, _, _ = self._slice_data_to_bounding_box(frozen_data_full, lats_full, lons_full, min_lat, max_lat, min_lon, max_lon)

            temp_grb = self._wave_grib.select(name='2 metre temperature')[0]
            temp_data_full, _, _ = temp_grb.data()
            temp_data, _, _ = self._slice_data_to_bounding_box(temp_data_full, lats_full, lons_full, min_lat, max_lat, min_lon, max_lon)
            temp_c = temp_data - 273.15  # Convert K to °C

            icing_mask = (frozen_data > 50) & (temp_c < 0)
            if np.any(icing_mask):
                indicators["icing_risk"] = True
            indicators["spatial_data"]["icing_mask"] = icing_mask
            indicators["details"]["max_frozen_precip"] = float(np.nanmax(frozen_data))
        except Exception as e:
            logger.error(f"Error processing icing risk: {e}", exc_info=True)

        # Extreme Temperatures
        try:
            cold_mask = temp_c < 0
            hot_mask = temp_c > 35
            if np.any(cold_mask):
                indicators["cold_risk"] = True
            if np.any(hot_mask):
                indicators["heat_risk"] = True
            indicators["spatial_data"]["cold_mask"] = cold_mask
            indicators["spatial_data"]["hot_mask"] = hot_mask
            indicators["details"]["min_temp_c"] = float(np.nanmin(temp_c))
            indicators["details"]["max_temp_c"] = float(np.nanmax(temp_c))
        except Exception as e:
            logger.error(f"Error processing temperature risks: {e}", exc_info=True)

        # Fog Risk
        try:
            rh_grb = self._wave_grib.select(name='2 metre relative humidity')[0]
            rh_data_full, _, _ = rh_grb.data()
            rh_data, _, _ = self._slice_data_to_bounding_box(rh_data_full, lats_full, lons_full, min_lat, max_lat, min_lon, max_lon)

            fog_mask = (rh_data > 95) & (vis_data < 1000)  # RH > 95% and vis < 1km
            if np.any(fog_mask):
                indicators["fog_risk"] = True
            indicators["spatial_data"]["fog_mask"] = fog_mask
            indicators["details"]["max_rh_percent"] = float(np.nanmax(rh_data))
        except Exception as e:
            logger.error(f"Error processing fog risk: {e}", exc_info=True)

        # Placeholder for Wave Data (to be added when provided)
        # Example: significant_wave_height_grb = grbs.select(name='Significant height of combined wind waves and swell')[0]
        # Add wave_mask for >2m, >4m, etc.

        return indicators

    def _generate_plot(self, lats: np.ndarray, lons: np.ndarray, wind_speed_knots: np.ndarray, 
                      min_lat: float, max_lat: float, min_lon: float, max_lon: float, 
                      valid_time: datetime, grib_info: GribFile, **kwargs) -> str:
        logger.info("Generating marine hazards plot")
        hazard_indicators = kwargs.get('hazard_indicators')

        # Create figure and axis with projection
        fig = plt.figure(figsize=(12, 8))
        ax = plt.axes(projection=ccrs.PlateCarree())
        ax.add_feature(cfeature.COASTLINE, linewidth=0.5)
        ax.add_feature(cfeature.BORDERS, linewidth=0.3, linestyle='--')
        ax.add_feature(cfeature.LAND, facecolor='lightgray', alpha=0.3)
        gl = ax.gridlines(draw_labels=True, linestyle='--', color='gray', alpha=0.5)
        gl.top_labels = False
        gl.right_labels = False

        # Set title
        title = (f'Marine Weather Hazards\n'
                 f'GFS {grib_info.metadata.cycle}, Valid: {valid_time.strftime("%Y-%m-%d %H:%M UTC")}\n'
                 f'Downloaded: {grib_info.download_time}')
        plt.title(title, pad=20, fontsize=14)

        # Legend handles
        legend_handles = []

        # Wind Gusts (color-coded areas)
        try:
            wind_25_33_mask = (wind_speed_knots >= 25) & (wind_speed_knots < 34)
            if np.any(wind_25_33_mask):
                ax.contourf(lons, lats, wind_25_33_mask, levels=[0.5, 1], colors='yellow', alpha=0.5, transform=ccrs.PlateCarree())
                wind_25_33_proxy = mlines.Line2D([], [], color='yellow', linewidth=2, label='Wind Gusts 25–33 kt')
                legend_handles.append(wind_25_33_proxy)

            wind_34_47_mask = (wind_speed_knots >= 34) & (wind_speed_knots <= 47)
            if np.any(wind_34_47_mask):
                ax.contourf(lons, lats, wind_34_47_mask, levels=[0.5, 1], colors='red', alpha=0.5, transform=ccrs.PlateCarree())
                wind_34_47_proxy = mlines.Line2D([], [], color='red', linewidth=2, label='Wind Gusts 34–47 kt')
                legend_handles.append(wind_34_47_proxy)

            wind_47_plus_mask = wind_speed_knots > 47
            if np.any(wind_47_plus_mask):
                ax.contourf(lons, lats, wind_47_plus_mask, levels=[0.5, 1], colors='purple', alpha=0.5, transform=ccrs.PlateCarree())
                wind_47_plus_proxy = mlines.Line2D([], [], color='purple', linewidth=2, label='Wind Gusts >47 kt')
                legend_handles.append(wind_47_plus_proxy)
            logger.debug("Added wind gust areas")
        except Exception as e:
            logger.error(f"Error adding wind gust areas: {e}", exc_info=True)

        # Hazard Indicators
        if hazard_indicators and "spatial_data" in hazard_indicators:
            spatial_data = hazard_indicators["spatial_data"]
            lats = spatial_data["lats"]
            lons = spatial_data["lons"]

            # Storm Potential
            if hazard_indicators["storm_potential"] and "storm_mask" in spatial_data:
                ax.contourf(lons, lats, spatial_data["storm_mask"], levels=[0.5, 1], colors='orange', alpha=0.5, transform=ccrs.PlateCarree())
                storm_proxy = mlines.Line2D([], [], color='orange', linewidth=2, label='Storm Potential')
                legend_handles.append(storm_proxy)

            # Severe Storm Risk
            if hazard_indicators["severe_storm_risk"] and "severe_mask" in spatial_data:
                ax.contourf(lons, lats, spatial_data["severe_mask"], levels=[0.5, 1], colors='darkviolet', alpha=0.5, transform=ccrs.PlateCarree())
                severe_proxy = mlines.Line2D([], [], color='darkviolet', linewidth=2, label='Severe Storm Risk')
                legend_handles.append(severe_proxy)

            # Low Visibility
            if hazard_indicators["low_visibility"] and "low_vis_mask" in spatial_data:
                ax.contourf(lons, lats, spatial_data["low_vis_mask"], levels=[0.5, 1], colors='gray', alpha=0.5, transform=ccrs.PlateCarree())
                vis_proxy = mlines.Line2D([], [], color='gray', linewidth=2, label='Low Visibility (<1 nm)')
                legend_handles.append(vis_proxy)

            # Icing Risk
            if hazard_indicators["icing_risk"] and "icing_mask" in spatial_data:
                ax.contourf(lons, lats, spatial_data["icing_mask"], levels=[0.5, 1], colors='blue', alpha=0.5, transform=ccrs.PlateCarree())
                icing_proxy = mlines.Line2D([], [], color='blue', linewidth=2, label='Icing Risk')
                legend_handles.append(icing_proxy)

            # Extreme Temperatures
            if hazard_indicators["cold_risk"] and "cold_mask" in spatial_data:
                ax.contourf(lons, lats, spatial_data["cold_mask"], levels=[0.5, 1], colors='cyan', alpha=0.5, transform=ccrs.PlateCarree())
                cold_proxy = mlines.Line2D([], [], color='cyan', linewidth=2, label='Cold Risk (<0°C)')
                legend_handles.append(cold_proxy)
            if hazard_indicators["heat_risk"] and "hot_mask" in spatial_data:
                ax.contourf(lons, lats, spatial_data["hot_mask"], levels=[0.5, 1], colors='darkred', alpha=0.5, transform=ccrs.PlateCarree())
                heat_proxy = mlines.Line2D([], [], color='darkred', linewidth=2, label='Heat Risk (>35°C)')
                legend_handles.append(heat_proxy)

            # Fog Risk
            if hazard_indicators["fog_risk"] and "fog_mask" in spatial_data:
                ax.contourf(lons, lats, spatial_data["fog_mask"], levels=[0.5, 1], colors='darkgray', alpha=0.5, transform=ccrs.PlateCarree())
                fog_proxy = mlines.Line2D([], [], color='darkgray', linewidth=2, label='Fog Risk')
                legend_handles.append(fog_proxy)

            # Placeholder for Wave Data
            # Example: if "wave_mask_2m" in spatial_data:
            #     ax.contourf(lons, lats, spatial_data["wave_mask_2m"], levels=[0.5, 1], colors='green', alpha=0.5, transform=ccrs.PlateCarree())
            #     wave_proxy = mlines.Line2D([], [], color='green', linewidth=2, label='Waves >2m')
            #     legend_handles.append(wave_proxy)

        # Add legend
        if legend_handles:
            ax.legend(handles=legend_handles, loc='upper right', fontsize=10, framealpha=0.8)

        # Set map extent
        ax.set_extent([min_lon, max_lon, min_lat, max_lat], crs=ccrs.PlateCarree())

        # Save plot to base64
        buf = BytesIO()
        plt.savefig(buf, format='png', bbox_inches='tight', dpi=150, pil_kwargs={'optimize': True, 'quality': 85})
        plt.close()
        buf.seek(0)
        image_base64 = base64.b64encode(buf.getvalue()).decode()
        logger.info("Marine hazards plot saved and encoded to base64")
        return image_base64

    def _generate_description(self, hazard_indicators: Dict, max_wind_speed: float, 
                             min_lat: float, max_lat: float, min_lon: float, max_lon: float) -> str:
        description_parts = ["This chart highlights marine weather hazards for sailors, focusing on wind gusts, storms, and other risks."]

        # Wind Gusts
        wind_desc = f"Wind gusts reach up to {max_wind_speed:.1f} knots. "
        if max_wind_speed >= 25:
            wind_desc += "Yellow areas show gusts of 25–33 knots, red areas 34–47 knots, and purple areas >47 knots, indicating hazardous conditions."
        else:
            wind_desc += "No significant wind gusts (≥25 knots) are expected."
        description_parts.append(wind_desc)

        # Storm Potential
        if hazard_indicators["storm_potential"]:
            description_parts.append("Orange areas indicate potential storms with heavy rain (>5 mm/h), instability (CAPE > 1000 J/kg), or radar reflectivity (>40 dBZ).")

        # Severe Storm Risk
        if hazard_indicators["severe_storm_risk"]:
            description_parts.append("Dark purple areas mark severe storm risks with high instability (CAPE > 1500 J/kg) and strong radar echoes (>50 dBZ).")

        # Low Visibility
        if hazard_indicators["low_visibility"]:
            description_parts.append("Gray areas show visibility below 1 nautical mile, posing navigation hazards.")

        # Icing Risk
        if hazard_indicators["icing_risk"]:
            description_parts.append("Blue areas indicate icing risk where frozen precipitation exceeds 50% and temperatures are below 0°C.")

        # Extreme Temperatures
        if hazard_indicators["cold_risk"]:
            description_parts.append("Cyan areas mark cold risks with temperatures below 0°C.")
        if hazard_indicators["heat_risk"]:
            description_parts.append("Dark red areas indicate heat risks with temperatures above 35°C.")

        # Fog Risk
        if hazard_indicators["fog_risk"]:
            description_parts.append("Dark gray areas show fog risk where relative humidity exceeds 95% and visibility is below 1 km.")

        # Summary
        if len(description_parts) > 1:
            description_parts.append("Sailors should exercise caution and monitor conditions closely.")
        else:
            description_parts.append("No significant hazards are expected, but vigilance is advised.")

        description = " ".join(description_parts)
        logger.info(f"Generated description: {description}")
        return description