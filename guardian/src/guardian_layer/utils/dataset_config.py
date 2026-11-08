"""Settings of the dataset loaders (#69), read from config/datasets.yaml.

The defaults below equal the shipped file. ``DatasetSettings(...)`` reads no file, so
tests build their own settings; ``get_dataset_settings()`` reads the file. Set
GUARDIAN_DATASETS_CONFIG to read another file (it must exist). Kept apart from
utils/config.py (lead's review on #87): the datasets are a large, separate group.
"""

import os
from functools import cache
from pathlib import Path
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, PositiveInt
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    YamlConfigSettingsSource,
)

from guardian_layer.utils.helpers import find_project_root
from guardian_layer.utils.schema import Anomalies


class IntelLabSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    folder: str = "intel_lab"
    data_file: str = "data.txt.gz"
    locations_file: str = "mote_locs.txt"
    # type -> (unit, lower_range, upper_range), in the file's column order; light
    # saturates at 1847.36 lux
    types: dict[str, tuple[str, float, float]] = {
        "temperature": ("C", 0.0, 50.0),
        "humidity": ("%", 0.0, 100.0),
        "light": ("lux", 0.0, 1847.36),
    }
    voltage_min: float = 2.4  # below this, readings go bad
    first_mote: int = 1
    last_mote: int = 54  # ids 55-58 in the file are not real motes
    bad_epoch: int = 65535  # 16-bit counter artefact
    origin_lat: float = 36.79  # mote x/y metres become offsets from here
    origin_lon: float = 10.18
    metres_per_degree: float = 111_320.0
    skipped: dict[str, str] = {"voltage": "housekeeping; used only for the fault label"}


class NabSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    folder: str = "nab"
    windows_file: str = "labels__combined_windows.json"
    # local file = local_prefix + NAB name with "/" as "__"
    local_prefix: str = "data__"
    # NAB name -> (station, type, unit). NAB states no units: likely mph, % and s
    # (MnDOT detectors), but none is documented
    series: dict[str, tuple[str, str, str]] = {
        "realTraffic/speed_6005.csv": ("6005", "speed", "unknown"),
        "realTraffic/speed_7578.csv": ("7578", "speed", "unknown"),
        "realTraffic/speed_t4013.csv": ("t4013", "speed", "unknown"),
        "realTraffic/occupancy_6005.csv": ("6005", "occupancy", "unknown"),
        "realTraffic/occupancy_t4013.csv": ("t4013", "occupancy", "unknown"),
        "realTraffic/TravelTime_387.csv": ("387", "travel_time", "unknown"),
        "realTraffic/TravelTime_451.csv": ("451", "travel_time", "unknown"),
    }
    skipped: dict[str, str] = {
        "realKnownCause/ambient_temperature_system_failure.csv": (
            "indoor office temperature, not in the Twin scenario"
        ),
        "realKnownCause/machine_temperature_system_failure.csv": (
            "inside an industrial machine"
        ),
        "realKnownCause/nyc_taxi.csv": "passenger counts, not a sensor",
    }


class ArrayOfThingsSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    folder: str = "array_of_things"
    data_file: str = "AoT_Chicago_2018-01-01_to_2019-12-31.csv.gz"
    time_format: str = "%Y/%m/%d %H:%M:%S"
    # shortcut: sensors.csv with the units is not in the local folder; read it once
    # it is added
    unit: str = "unknown"
    skipped_parameters: dict[str, str] = {"id": "a chip id, not a measurement"}
    # sensor name -> why it is left out
    skipped_sensors: dict[str, str] = {
        "spv1840lr5h_b": "microphone: noise is not related to flooding",
    }
    skipped: dict[str, str] = {
        "value_raw": "uncalibrated; value_hrf is the calibrated value",
    }


class TrafficSpeedsSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    folder: str = "traffic_speeds"
    chicago_file: str = "chicago_bus_regions.csv"
    nyc_file: str = "nyc_links.csv"
    unit: str = "unknown"  # neither dataset page states the speed unit
    skipped: dict[str, str] = {
        "bus_count": "Chicago metadata; 0 marks a row with no measurement",
        "num_reads": "metadata of the Chicago estimate",
        "travel_time": "derived from speed; 0 marks a NYC row with no measurement",
        "status": "not a measurement",
        "id": "row id",
    }


class UkTrafficSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    folder: str = "uk_traffic"
    pattern: str = "site_*.json"
    # column -> (type, unit); the units are in the column names
    columns: dict[str, tuple[str, str]] = {
        "Avg mph": ("average_speed", "mph"),
        "Total Volume": ("volume", "vehicles"),
    }
    skipped: dict[str, str] = {
        "length and speed bins": "a breakdown of Total Volume",
        "Time Interval": "row index within the day",
        "Site Name": "the file name is the station",
    }


class UkFloodSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    folder: str = "uk_flood"
    pattern: str = "readings-full-*.csv"
    # parameter value -> why it is left out
    skipped_parameters: dict[str, str] = {
        "flow": "computed from level, adds nothing",
    }
    skipped: dict[str, str] = {
        "date": "part of dateTime",
        "label": "station name text",
        "station": "station URL; the measure id names the station",
        "valueType": "part of the measure id",
        "period": "part of the measure id",
        "datumType": "part of the measure id",
        "location": "lat/lon need the stations API",
    }


class UsgsSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    folder: str = "usgs_water"
    pattern: str = "*.rdb"
    # parameter code -> (type, unit); the units are in each file's header
    parameters: dict[str, tuple[str, str]] = {
        "00065": ("gage_height", "ft"),
    }
    # tz_cd -> UTC offset
    time_zones: dict[str, str] = {
        "EST": "-05:00",
        "EDT": "-04:00",
        "CST": "-06:00",
        "CDT": "-05:00",
        "MST": "-07:00",
        "MDT": "-06:00",
        "PST": "-08:00",
        "PDT": "-07:00",
    }
    skipped: dict[str, str] = {
        "00010": "water quality, not a flood reading",
        "00095": "water quality, not a flood reading",
        "00300": "water quality, not a flood reading",
        "00400": "water quality, not a flood reading",
        "63680": "water quality, not a flood reading",
        "99133": "water quality, not a flood reading",
        "99137": "water quality, not a flood reading",
        "32295": "water quality, not a flood reading",
        "32315": "water quality, not a flood reading",
        "32321": "water quality, not a flood reading",
        "00060": "discharge, computed from gage height",
        "72137": "tidally filtered discharge, computed",
        "63160": "water level elevation, gage height plus a datum",
        "72147": "depth of the sensor below the surface (housekeeping)",
        "72255": "mean velocity computed for discharge",
        "80297": "sediment load computed by regression",
        "99246": "prediction limit of a regression",
        "99247": "prediction limit of a regression",
        "99409": "sediment concentration estimated by regression",
        "_cd": "qualifier columns (only P, provisional)",
    }


class NoaaTidesSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    folder: str = "noaa_tides"
    pattern: str = "*_*.csv"  # <station>_<product>.csv
    # product -> value column -> (type, unit); metric, as the values show (CO-OPS
    # units=metric: mb, °C, m, m/s)
    products: dict[str, dict[str, tuple[str, str]]] = {
        "air_pressure": {"Pressure": ("air_pressure", "mb")},
        "air_temperature": {"Air Temperature": ("air_temperature", "°C")},
        "water_temperature": {"Water Temperature": ("water_temperature", "°C")},
        "water_level": {"Water Level": ("water_level", "m")},
        "wind": {
            "Speed": ("wind_speed", "m/s"),
            "Direction": ("wind_direction", "°"),
            "Gust": ("wind_gust", "m/s"),
        },
    }
    # flag column set to 1 -> label, checked in this order (CO-OPS response help):
    # X/N max/min expected exceeded, L water level limit, R rate of change, F flat
    flag_labels: dict[str, Anomalies] = {
        "X": Anomalies.OUT_OF_RANGE,
        "N": Anomalies.OUT_OF_RANGE,
        "L": Anomalies.OUT_OF_RANGE,
        "R": Anomalies.SENSOR_MALFUNCTION,
        "F": Anomalies.SENSOR_MALFUNCTION,
    }
    skipped: dict[str, str] = {
        "Sigma": "spread of the 1 s samples, not a measurement",
        "O or I (for verified)": "a count of samples outside 3 sigma, not a flag",
        "Quality": "preliminary or verified",
        "Direction (second)": "compass text of the numeric direction",
    }


class BellingeSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    folder: str = "bellinge"
    # cleaned SCADA series: <location>[p<n>]_<measurement>_<system>p<period>_proc_v6.csv
    cleaned_zip: str = "2_cleaned_data.zip"
    file_pattern: str = (
        r"(?P<location>[A-Z0-9]+)(?P<pump>p\d+)?_(?:(?P<measurement>[A-Za-z0-9]+)_)?"
        r"(?P<system>iFix|System2000|Danova)p(?P<period>\d+)_proc_v6\.csv"
    )
    measurements: tuple[str, ...] = (
        "Level1",
        "Level2",
        "LevelBasin",
        "LevelInlet",
        "LevelPS",
    )
    danova_measurement: str = "Level"  # Danova files name no measurement
    level_kind: str = "water_depth"
    level_unit: str = "m"  # 2_Sensordata_v2.pdf; both systems give 0-2 m after cleaning
    # G71F68Yp1 is the flow meter in the pressure pipe of the G71F68Y pumps (ESSD
    # 13, 4779, Table 1; design capacity 80 L/s). Sensordata_v2.pdf gives m3/h for the
    # raw series but the cleaned values run 0.06-0.075 while pumping: 60-75 L/s, so
    # m3/s (inferred, the cleaning scripts are not in the local folder). It runs about
    # 3% of the time, so its frozen flags (a pump at constant speed) are not faults:
    # it gets no label
    pump_kind: str = "pump_flow"
    pump_unit: str = "m3/s"
    skip_flags: tuple[str, ...] = ("ffill", "stamp", "man_remove")
    # flag -> label, checked in this order after the skip flags
    flag_labels: dict[str, Anomalies] = {
        "outbound": Anomalies.OUT_OF_RANGE,
        "frozen": Anomalies.SENSOR_MALFUNCTION,
        "frozen_high": Anomalies.SENSOR_MALFUNCTION,
        "outlier": Anomalies.SENSOR_MALFUNCTION,
    }
    # rain gauges: zip member -> (station, time format)
    gauges_zip: str = "3a_Raingauges.zip"
    gauges: dict[str, tuple[str, str]] = {
        "#3a_Raingauges/5425_ts.txt": ("5425", "%Y-%m-%d %H:%M:%S"),
        "#3a_Raingauges/5427_ts.txt": ("5427", "%Y-%m-%d %H:%M:%S"),
        "#3a_Raingauges/Aabakken_bellinge_vandvaerk_v2_ts.txt": (
            "aabakken",
            "%d-%m-%Y %H:%M:%S",
        ),
    }
    gauge_kind: str = "rain_intensity"
    gauge_unit: str = "µm/s"  # the _ts.txt header: mu-m/sec
    # a gauge's periods with no or unaccepted data (UTC): readings inside are skipped
    period_files: tuple[str, ...] = (
        "_missing_data.txt",
        "_planned_closing_and_not_accepted_data.txt",
    )
    period_line: str = r"^(\d{8} \d{2}:\d{2})\s+(\d{8} \d{2}:\d{2})"  # start, stop
    period_time_format: str = "%Y%m%d %H:%M"
    skipped: dict[str, str] = {
        "position": "throttle position: a gate actuator, not a sensor",
        "value_no_errors": "derived from raw_value and the flags",
        "depth_s": "derived from raw_value",
        "level": "depth plus the 0-point",
        ".kmd": "the same rain series in an undocumented format",
        "location": "coordinates are UTM: converting needs a new dependency",
    }


class ChicagoBeachSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    folder: str = "chicago_beach"
    time_column: str = "Measurement Timestamp"
    time_format: str = "%m/%d/%Y %I:%M:%S %p"
    # no-data sentinels, matched by value: -100000 in any column; 999.9 only in the
    # wind columns (999.9 hPa is a real pressure there, about as common as 999.8)
    missing: tuple[str, ...] = ("-100000",)
    column_missing: dict[str, tuple[str, ...]] = {
        "Wind Speed": ("999.9",),
        "Maximum Wind Speed": ("999.9",),
    }
    # file -> (station column, column -> (type, unit)); units from the Chicago Data
    # Portal column descriptions
    files: dict[str, tuple[str, dict[str, tuple[str, str]]]] = {
        "weather_stations.csv": (
            "Station Name",
            {
                "Air Temperature": ("air_temperature", "°C"),
                "Wet Bulb Temperature": ("wet_bulb_temperature", "°C"),
                "Humidity": ("humidity", "%"),
                "Rain Intensity": ("rain_intensity", "mm/h"),
                "Interval Rain": ("interval_rain", "mm"),
                "Wind Direction": ("wind_direction", "°"),
                "Wind Speed": ("wind_speed", "m/s"),
                "Maximum Wind Speed": ("max_wind_speed", "m/s"),
                "Barometric Pressure": ("barometric_pressure", "hPa"),
            },
        ),
    }
    skipped: dict[str, str] = {
        "water_sensors.csv": "lake conditions (waves, turbidity, depth), not a flood",
        "Solar Radiation": "not related to flooding",
        "Battery Life": "housekeeping",
        "Heading": "housekeeping: the wind unit's heading",
        "Precipitation Type": "categorical",
        "Total Rain": "cumulative of Interval Rain since midnight",
        "Measurement Timestamp Label": "text copy of the timestamp",
        "Measurement ID": "record id",
    }


class GridFrequencySettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    folder: str = "grid_frequency"
    # FrequencyData__<station>__<file>; the Numbers.txt summaries are not data. The
    # 100 ms zips are grid_frequency_100ms
    patterns: tuple[str, ...] = ("FrequencyData__*__*.csv",)
    # station keys left out (matched case-insensitively inside the key)
    skipped_keys: dict[str, str] = {
        "sync01": "a 1 s copy of the 100 ms DE_KA, DE_OL, PT_LI, TUR_IS recordings",
        "solar_eclipse": "no timestamps",
    }
    # quality indicator 1 = invalid, 2 = interpolated (Jumar et al., Table 3)
    bad_qi: tuple[str, ...] = ("1", "2")
    kind: str = "frequency_deviation"
    unit: str = "mHz"  # f - f_nominal, the paper's Table 2; times are UTC
    skipped: dict[str, str] = {
        "reference columns": "the DE_KA series repeated in other stations' files",
    }


class EttSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    folder: str = "ett_transformer"
    files: dict[str, str] = {"ETTm1.csv": "transformer1", "ETTm2.csv": "transformer2"}
    # loads (high/middle/low useful and useless): type = lower case
    columns: tuple[str, ...] = ("HUFL", "HULL", "MUFL", "MULL", "LUFL", "LULL")
    unit: str = "unknown"  # the ETT release states no units
    skipped: dict[str, str] = {
        "OT": "oil temperature inside a transformer, not an environment reading",
    }


class UciAirQualitySettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    folder: str = "uci_air_quality"
    zip_file: str = "data.zip"
    member: str = "AirQualityUCI.csv"
    time_format: str = "%d/%m/%Y %H.%M.%S"
    station: str = "multisensor"  # one device in an Italian city
    missing: tuple[str, ...] = ("-200",)
    # column -> (type, unit), from the UCI page; GT = reference analyser, PT08 = the
    # device's metal-oxide sensor responses (no unit stated)
    columns: dict[str, tuple[str, str]] = {
        "CO(GT)": ("co_gt", "mg/m³"),
        "PT08.S1(CO)": ("pt08_s1_co", "unknown"),
        "NMHC(GT)": ("nmhc_gt", "µg/m³"),
        "C6H6(GT)": ("c6h6_gt", "µg/m³"),
        "PT08.S2(NMHC)": ("pt08_s2_nmhc", "unknown"),
        "NOx(GT)": ("nox_gt", "ppb"),
        "PT08.S3(NOx)": ("pt08_s3_nox", "unknown"),
        "NO2(GT)": ("no2_gt", "µg/m³"),
        "PT08.S4(NO2)": ("pt08_s4_no2", "unknown"),
        "PT08.S5(O3)": ("pt08_s5_o3", "unknown"),
        "T": ("temperature", "°C"),
        "RH": ("humidity", "%"),
    }
    skipped: dict[str, str] = {
        "AH": "absolute humidity, derived from T and RH",
        "trailing columns": "two empty columns after AH",
    }


class UciLoadSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    folder: str = "uci_load_370_meters"
    zip_file: str = "data.zip"
    member: str = "LD2011_2014.txt"
    kind: str = "load"
    unit: str = "kW"  # UCI page: "values in kW of each 15 min"
    skipped: dict[str, str] = {
        "leading zeros": "a meter's zeros before its first reading: not installed yet",
    }


class UciHouseholdSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    folder: str = "uci_household_power"
    zip_file: str = "data.zip"
    member: str = "household_power_consumption.txt"
    time_format: str = "%d/%m/%Y %H:%M:%S"
    station: str = "household"  # one house near Paris
    missing: tuple[str, ...] = ("?",)
    # column -> (type, unit), from the UCI page
    columns: dict[str, tuple[str, str]] = {
        "Global_active_power": ("global_active_power", "kW"),
        "Global_reactive_power": ("global_reactive_power", "kW"),
        "Voltage": ("voltage", "V"),
        "Sub_metering_1": ("sub_metering_1", "Wh"),
        "Sub_metering_2": ("sub_metering_2", "Wh"),
        "Sub_metering_3": ("sub_metering_3", "Wh"),
    }
    skipped: dict[str, str] = {
        "Global_intensity": "current, about active power divided by voltage",
    }


class BeijingSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    folder: str = "beijing_air_quality"
    zip_file: str = "data.zip"
    inner_zip: str = "PRSA2017_Data_20130301-20170228.zip"  # the 12 station CSVs
    missing: tuple[str, ...] = ("NA",)
    # column -> (type, unit), from the UCI page
    columns: dict[str, tuple[str, str]] = {
        "PM2.5": ("pm25", "µg/m³"),
        "PM10": ("pm10", "µg/m³"),
        "SO2": ("so2", "µg/m³"),
        "NO2": ("no2", "µg/m³"),
        "CO": ("co", "µg/m³"),
        "O3": ("o3", "µg/m³"),
        "TEMP": ("temperature", "°C"),
        "PRES": ("pressure", "hPa"),
        "DEWP": ("dew_point", "°C"),
        "RAIN": ("rain", "mm"),
        "WSPM": ("wind_speed", "m/s"),
    }
    skipped: dict[str, str] = {
        "No": "row number",
        "wd": "wind direction as compass text (categorical)",
        "year/month/day/hour": "the time",
        "data.csv, test.csv": "stock prices in the same archive, not sensor data",
    }


class JenaSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    folder: str = "jena_weather"
    pattern: str = "mpi_roof_*.zip"  # one CSV per zip, named like the zip
    encoding: str = "latin-1"  # the header's unit symbols (µ, ²) are latin-1
    time_format: str = "%d.%m.%Y %H:%M:%S"
    station: str = "mpi_roof"  # the Max Planck Institute roof station
    missing: tuple[str, ...] = ("-9999", "-9999.00")
    # header -> (type, unit); the units are in the headers
    columns: dict[str, tuple[str, str]] = {
        "p (mbar)": ("pressure", "mbar"),
        "T (degC)": ("temperature", "°C"),
        "rh (%)": ("humidity", "%"),
        "wv (m/s)": ("wind_speed", "m/s"),
        "max. wv (m/s)": ("max_wind_speed", "m/s"),
        "wd (deg)": ("wind_direction", "°"),
        "rain (mm)": ("rain", "mm"),
        "raining (s)": ("raining_time", "s"),
    }
    skipped: dict[str, str] = {
        "SWDR (W/m²), PAR (µmol/m²/s), max. PAR (µmol/m²/s), CO2 (ppm)": (
            "solar radiation, PAR and CO2: not related to flooding"
        ),
        "Tpot, Tdew, VPmax, VPact, VPdef, sh, H2OC, rho": "derived from p, T and rh",
        "Tlog": "the logger's own temperature (housekeeping)",
    }


class SensorCommunitySettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    folder: str = "sensor_community"
    pattern: str = "*.csv"  # <date>_<model>_sensor_<id>.csv, one day each
    # column -> quantity; type = <model>_<quantity>. P0/P1/P2/P4 follow the network's
    # convention (PM1, PM10, PM2.5, PM4); no data dictionary states it or the units
    columns: dict[str, str] = {
        "pressure": "pressure",
        "temperature": "temperature",
        "humidity": "humidity",
        "P0": "pm1",
        "P1": "pm10",
        "P2": "pm25",
        "P4": "pm4",
        "N05": "number_pm05",
        "N1": "number_pm1",
        "N25": "number_pm25",
        "N4": "number_pm4",
        "N10": "number_pm10",
    }
    unit: str = "unknown"
    skipped: dict[str, str] = {
        "noise_LAeq, noise_LA_min, noise_LA_max": "noise: not related to flooding",
        "counts_per_minute": "radiation counter: not related to flooding",
        "co2_ppm": "CO2: not related to flooding",
        "altitude, pressure_sealevel": "empty or derived from pressure",
        "durP1, ratioP1, durP2, ratioP2": "raw pulse timing of the PM sensor",
        "TS": "typical particle size, derived",
        "noise_LA01, noise_LA95": "percentiles of the noise level, derived",
        "hv_pulses, tube, counts, sample_time_ms": "radiation counter housekeeping",
        "location": "location id; lat/lon are kept",
    }


class MelbourneSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    folder: str = "melbourne"
    microclimate_file: str = "microclimate.csv"
    # column -> type; the City of Melbourne open data API states no units
    microclimate_columns: dict[str, str] = {
        "minimumwinddirection": "wind_direction_min",
        "averagewinddirection": "wind_direction_avg",
        "maximumwinddirection": "wind_direction_max",
        "minimumwindspeed": "wind_speed_min",
        "averagewindspeed": "wind_speed_avg",
        "gustwindspeed": "wind_speed_gust",
        "airtemperature": "air_temperature",
        "relativehumidity": "humidity",
        "atmosphericpressure": "pressure",
        "pm25": "pm25",
        "pm10": "pm10",
    }
    microclimate_unit: str = "unknown"
    skipped: dict[str, str] = {
        "noise": "not related to flooding",
        "pedestrian_counts.csv": "pedestrian counts are not a Twin sensor type",
    }


class CallSourceSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    file: str
    source: str  # the loader's cluster prefix: one per city
    time_column: str
    zone_column: str
    # the real flood, read as UTC like every source time: start included, end not
    event_start: str
    event_end: str
    # column -> value of the rows left out
    exclude: dict[str, str] = {}


class EmergencyCallsSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    folder: str = "emergency_calls"
    kind: str = "call_count"
    unit: str = "calls/min"
    # NYC EMS (Ida flash flood, 1 Sept 2021; borough as the zone) and New Orleans calls
    # for service (Hurricane Ida, 29 Aug 2021; police district as the zone; CC BY, no
    # commercial use). Both files are API extracts of the weeks around the flood
    sources: tuple[CallSourceSettings, ...] = (
        CallSourceSettings(
            file="nyc_ems.csv",
            source="nyc_ems",
            time_column="incident_datetime",
            zone_column="borough",
            event_start="2021-09-01T00:00:00",
            event_end="2021-09-02T00:00:00",
        ),
        CallSourceSettings(
            file="nola_calls.csv",
            source="nola_calls",
            time_column="timecreate",
            zone_column="policedistrict",
            event_start="2021-08-29T00:00:00",
            event_end="2021-08-30T00:00:00",
            exclude={"selfinitiated": "Y"},
        ),
    )
    skipped: dict[str, str] = {
        "incident_dispatch_area": "NYC: a finer zone than the borough, not used",
        "location, mapx, mapy": "NOLA: the call's point; the district is the zone",
        "selfinitiated": "NOLA: Y marks an officer's own stop, not a public call",
    }


class FaultSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    spike_count_max: int = 3  # most spikes in one window
    # (low, high) ranges drawn per fault; "spreads" are multiples of the type's normal
    # within-window spread (median std of clean training sensor-windows of that type)
    spike_spreads: tuple[float, float] = (5.0, 10.0)  # size of a spike
    drift_spreads: tuple[float, float] = (3.0, 6.0)  # size of a drift
    noise_spreads: tuple[float, float] = (1.0, 3.0)  # size of the added noise
    oscillation_spreads: tuple[float, float] = (2.0, 4.0)  # size of an oscillation
    period_readings: tuple[float, float] = (4.0, 8.0)  # oscillation and sine period
    spoof_spreads: tuple[float, float] = (3.0, 6.0)  # size of the spoof offset
    sine_spreads: tuple[float, float] = (2.0, 4.0)  # size of a sine
    ramp_spreads: tuple[float, float] = (3.0, 6.0)  # size of a ramp
    pattern_values: tuple[int, int] = (2, 4)  # motif length, both ends included
    pattern_spreads: float = 3.0  # motif values lie within this many spreads


class InjectionSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    # time slots per window (a sensor can have fewer readings in a window)
    window_slots: int = 20
    # window length in seconds; set, it replaces window_slots (max(1, round(
    # window_seconds / period)) slots) and readings keep their real spacing; unset
    # keeps window_slots one-second slots (#95)
    window_seconds: Annotated[float, Field(gt=0, allow_inf_nan=False)] | None = None
    # share of the eligible (window, station) pairs, earliest first, that train (one
    # time cut for the whole source, not per sensor)
    train_share: float = 0.7
    inject_share: float = 0.10  # share of windows that get a fault
    # the fault kinds drawn from, in this order (the order fixes the random draws)
    fault_kinds: tuple[str, ...] = (
        "stuck",
        "spike",
        "drift",
        "noise",
        "oscillation",
        "spoof",
    )
    # fabricated signals that replace a whole window; they are not sensor data
    fabricated_kinds: tuple[str, ...] = ("sine", "ramp", "pattern", "uniform")
    # kind -> label; spoof is absent on purpose (the static check owns spoofed_data)
    labels: dict[str, Anomalies] = {
        "stuck": Anomalies.SENSOR_MALFUNCTION,
        "spike": Anomalies.SENSOR_MALFUNCTION,
        "drift": Anomalies.SENSOR_MALFUNCTION,
        "noise": Anomalies.SENSOR_MALFUNCTION,
        "oscillation": Anomalies.SENSOR_MALFUNCTION,
        "sine": Anomalies.NOT_SENSOR_DATA,
        "ramp": Anomalies.NOT_SENSOR_DATA,
        "pattern": Anomalies.NOT_SENSOR_DATA,
        "uniform": Anomalies.NOT_SENSOR_DATA,
    }
    faults: FaultSettings = FaultSettings()
    # source name -> measured seconds between one sensor's readings (median gap over the
    # first 500,000 readings, whole seconds; Intel Lab keeps the notebook's 31.0)
    sample_seconds: dict[str, float] = {
        "array_of_things": 24.0,  # ~3,103,232,571 readings (estimate)
        "beijing": 3600.0,  # 4,556,243 readings
        "bellinge": 60.0,  # 43,684,104 readings
        "chicago_beach": 3600.0,  # 1,724,650 readings
        "emergency_calls": 60.0,  # 563,882 readings
        "ett": 900.0,  # 836,160 readings
        # 216,976,068 readings, the 100 ms recordings included
        "grid_frequency": 1.0,
        "grid_frequency_100ms": 0.1,  # 126,210,910 lines (raw count)
        "intel_lab": 31.0,  # 6,630,162 readings
        "jena": 600.0,  # 842,088 readings
        "melbourne": 901.0,  # 10,317,038 readings
        "nab": 300.0,  # 15,664 readings
        "noaa_tides": 360.0,  # 74,400 readings
        "sensor_community": 146.0,  # 840,504 readings
        "traffic_speeds": 615.0,  # 1,620,335 readings
        "uci_air_quality": 3600.0,  # 95,949 readings
        "uci_household": 60.0,  # 12,295,680 readings
        "uci_load": 900.0,  # 41,936,458 readings
        "uk_flood": 900.0,  # 1,394,777 readings
        "uk_traffic": 900.0,  # 59,698 readings
        "usgs": 300.0,  # 25,746 readings
    }
    # a memory cap (about 3-6 GB at the cap); only sources above it are sampled in hour
    # blocks
    max_readings: int = 50_000_000  # most readings kept from one source
    block_hours: int = 2  # hours kept in each sampling block
    # source name -> keep one block in this many hours; only sources over max_readings.
    # For such a source, the smallest every_hours >= 1.2 * total * block_hours /
    # 10,000,000 that shares no factor with 24, so block starts move through every hour
    # of the day and every weekday.
    sample_every_hours: dict[str, PositiveInt] = {
        "array_of_things": 745,  # about 8.3M readings kept
        # from a total that counted the 100 ms recordings: about 3.6M readings kept
        "grid_frequency": 53,
        "grid_frequency_100ms": 31,  # about 8.1M readings kept
    }
    # a sensor reporting faster than this fraction of its source's sample period
    # is dropped
    fast_sensor_share: float = 0.5


class LongInjectionSettings(BaseModel):
    """What the long-window model (#96) changes in ``injection``; the rest is shared."""

    model_config = ConfigDict(extra="forbid")

    # the grid's bucket means smooth spike, noise and oscillation away; drift and stuck
    # build up over hours
    fault_kinds: tuple[str, ...] = ("stuck", "drift")
    fabricated_kinds: tuple[str, ...] = ("sine", "ramp", "pattern", "uniform")
    # a block must hold a whole 6 h or 24 h window
    block_hours: int = 24
    # merged into injection's: 1,801 shares no factor with 24 and is no multiple of
    # 168, so the blocks fall on every hour and weekday (about 10 days of the source)
    sample_every_hours: dict[str, PositiveInt] = {"array_of_things": 1801}


class DatasetSettings(BaseSettings):
    model_config = SettingsConfigDict(extra="forbid")

    # the shared datasets folder next to the repository; relative to the project root,
    # or absolute
    root: str = "../datasets"
    intel_lab: IntelLabSettings = IntelLabSettings()
    nab: NabSettings = NabSettings()
    array_of_things: ArrayOfThingsSettings = ArrayOfThingsSettings()
    traffic_speeds: TrafficSpeedsSettings = TrafficSpeedsSettings()
    uk_traffic: UkTrafficSettings = UkTrafficSettings()
    uk_flood: UkFloodSettings = UkFloodSettings()
    usgs: UsgsSettings = UsgsSettings()
    noaa_tides: NoaaTidesSettings = NoaaTidesSettings()
    bellinge: BellingeSettings = BellingeSettings()
    chicago_beach: ChicagoBeachSettings = ChicagoBeachSettings()
    grid_frequency: GridFrequencySettings = GridFrequencySettings()
    # DE_KA01, DE_OL01, PT_LI01 and TUR_IS01 at 10 readings a second, 2019-07/08
    grid_frequency_100ms: GridFrequencySettings = GridFrequencySettings(
        patterns=("FrequencyData__*__*_100ms.zip",)
    )
    ett: EttSettings = EttSettings()
    uci_air_quality: UciAirQualitySettings = UciAirQualitySettings()
    uci_load: UciLoadSettings = UciLoadSettings()
    uci_household: UciHouseholdSettings = UciHouseholdSettings()
    beijing: BeijingSettings = BeijingSettings()
    jena: JenaSettings = JenaSettings()
    sensor_community: SensorCommunitySettings = SensorCommunitySettings()
    melbourne: MelbourneSettings = MelbourneSettings()
    emergency_calls: EmergencyCallsSettings = EmergencyCallsSettings()
    injection: InjectionSettings = InjectionSettings()
    long_injection: LongInjectionSettings = LongInjectionSettings()

    def long_window(self, window_seconds: float) -> "DatasetSettings":
        """A copy for the long-window model: ``long_injection`` applied to
        ``injection``, windows of ``window_seconds``."""
        override = self.long_injection
        injection = InjectionSettings.model_validate(
            {
                **self.injection.model_dump(),
                "window_seconds": window_seconds,
                "fault_kinds": override.fault_kinds,
                "fabricated_kinds": override.fabricated_kinds,
                "block_hours": override.block_hours,
                "sample_every_hours": {
                    **self.injection.sample_every_hours,
                    **override.sample_every_hours,
                },
            }
        )
        return self.model_copy(update={"injection": injection})

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        *args: PydanticBaseSettingsSource,
        **kwargs: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        # init arguments only: read_dataset_settings() passes the YAML's values in, so
        # settings a test builds hold the defaults; env vars and dotenv are left out
        return (init_settings,)


def read_dataset_settings() -> DatasetSettings:
    """config/datasets.yaml, or the file GUARDIAN_DATASETS_CONFIG names, as settings."""
    override = os.environ.get("GUARDIAN_DATASETS_CONFIG")
    if override:
        path = Path(override)
        if not path.is_file():
            raise FileNotFoundError(f"GUARDIAN_DATASETS_CONFIG file not found: {path}")
    else:
        root = find_project_root()
        if root is None:  # no repository around (installed package): use defaults
            return DatasetSettings()
        path = root / "config" / "datasets.yaml"
    # explicit: Windows would read the file in its own code page ("°" -> "Â°");
    # a missing or empty file gives {}
    yaml = YamlConfigSettingsSource(
        DatasetSettings, yaml_file=path, yaml_file_encoding="utf-8"
    )
    return DatasetSettings(**yaml())


@cache
def get_dataset_settings() -> DatasetSettings:
    return read_dataset_settings()
