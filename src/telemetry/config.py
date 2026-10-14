"""Fleet layout and sensor model for the synthetic plant telemetry. All data is synthetic."""

PLANTS = {
    "PLT-N": "Plant North",
    "PLT-S": "Plant South",
    "PLT-E": "Plant East",
}
LINES = ["A", "B", "C", "D"]
STATION_TYPES_BY_POSITION = [
    "press", "cnc_mill", "welder", "robot_arm",
    "conveyor", "cnc_mill", "welder", "press",
]

SENSORS = [
    "vibration_rms",
    "bearing_temp_c",
    "motor_current_a",
    "spindle_rpm",
    "hydraulic_pressure_bar",
    "acoustic_db",
    "cycle_time_s",
]

# Nominal operating point per station type.
NOMINAL = {
    "press":     dict(vibration_rms=2.0, bearing_temp_c=45, motor_current_a=30,  spindle_rpm=1200, hydraulic_pressure_bar=180, acoustic_db=78, cycle_time_s=12),
    "cnc_mill":  dict(vibration_rms=1.5, bearing_temp_c=50, motor_current_a=18,  spindle_rpm=9000, hydraulic_pressure_bar=60,  acoustic_db=72, cycle_time_s=45),
    "welder":    dict(vibration_rms=0.8, bearing_temp_c=60, motor_current_a=120, spindle_rpm=300,  hydraulic_pressure_bar=6,   acoustic_db=70, cycle_time_s=20),
    "robot_arm": dict(vibration_rms=1.0, bearing_temp_c=42, motor_current_a=12,  spindle_rpm=3000, hydraulic_pressure_bar=6,   acoustic_db=65, cycle_time_s=15),
    "conveyor":  dict(vibration_rms=1.2, bearing_temp_c=38, motor_current_a=8,   spindle_rpm=1450, hydraulic_pressure_bar=6,   acoustic_db=68, cycle_time_s=5),
}

# Relative noise (std / nominal) per sensor.
NOISE = dict(
    vibration_rms=0.08, bearing_temp_c=0.02, motor_current_a=0.03, spindle_rpm=0.01,
    hydraulic_pressure_bar=0.02, acoustic_db=0.015, cycle_time_s=0.03,
)

FAILURE_MODES_BY_TYPE = {
    "press": ["seal_leak"],
    "cnc_mill": ["bearing_wear", "overheating"],
    "welder": ["overheating"],
    "robot_arm": ["bearing_wear", "overheating"],
    "conveyor": ["bearing_wear"],
}
