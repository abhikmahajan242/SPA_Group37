import json
import random
from datetime import datetime, timedelta

def generate_urbanpulse_streams(num_records=5000):
    start_time = datetime.now()
    
    # Contextual urban zones for the simulation
    zones = ["Cyber Hub", "Golf Course Road", "Sector 48", "Sector 65", "Sector 68", "Sector 70"]
    routes = ["R-101", "R-102", "R-103", "R-104", "R-105"]
    signal_phases = ["RED", "GREEN", "YELLOW"]
    
    # Output file definitions
    files = {
        "bus": "urbanpulse_bus_gps.jsonl",
        "traffic": "urbanpulse_traffic_signals.jsonl",
        "aqi": "urbanpulse_air_quality.jsonl",
        "meters": "urbanpulse_smart_meters.jsonl"
    }
    
    # Open all files in write mode simultaneously
    with open(files["bus"], "w") as f_bus, \
         open(files["traffic"], "w") as f_traffic, \
         open(files["aqi"], "w") as f_aqi, \
         open(files["meters"], "w") as f_meters:
         
        for i in range(num_records):
            # Increment time by 2 seconds per record to simulate steady stream
            current_time = (start_time + timedelta(seconds=i * 2)).isoformat()
            
            # 1. Bus GPS Data
            bus_record = {
                "bus_id": f"BUS-{random.randint(1000, 1999)}",
                "route_id": random.choice(routes),
                "lat": round(random.uniform(28.35, 28.50), 6), # Constrained to match the urban zones
                "lon": round(random.uniform(76.95, 77.10), 6),
                "speed_kmh": random.randint(0, 65),
                "occupancy_pct": random.randint(10, 100),
                "timestamp": current_time
            }
            f_bus.write(json.dumps(bus_record) + "\n")
            
            # 2. Traffic Signals Data
            traffic_record = {
                "junction_id": f"JUNC-{random.randint(100, 500)}",
                "zone": random.choice(zones),
                "vehicle_count": random.randint(0, 150),
                "avg_wait_sec": random.randint(10, 240),
                "signal_phase": random.choice(signal_phases),
                "timestamp": current_time
            }
            f_traffic.write(json.dumps(traffic_record) + "\n")
            
            # 3. Air Quality Data (Includes the 5% null AQI fault injection)
            pm25_val = random.randint(40, 350)
            # Inject None (null) if the random threshold falls in the 5% bracket
            aqi_val = None if random.random() <= 0.05 else pm25_val + random.randint(10, 50)
            
            aqi_record = {
                "sensor_id": f"AQI-{random.randint(1, 100)}",
                "zone": random.choice(zones),
                "pm25": pm25_val,
                "pm10": pm25_val + random.randint(20, 80),
                "no2": random.randint(10, 100),
                "aqi": aqi_val,
                "timestamp": current_time
            }
            f_aqi.write(json.dumps(aqi_record) + "\n")
            
            # 4. Smart Meters Data
            meter_record = {
                "meter_id": f"METER-{random.randint(10000, 99999)}",
                "ward_id": f"WARD-{random.randint(1, 20)}",
                "kwh_reading": round(random.uniform(100.0, 5000.0), 2),
                "voltage": random.choice([220, 230, 240]),
                "power_factor": round(random.uniform(0.8, 1.0), 2),
                "timestamp": current_time
            }
            f_meters.write(json.dumps(meter_record) + "\n")

    print(f"Success! Generated {num_records} records per stream.")
    print("Files saved: urbanpulse_bus_gps.jsonl, urbanpulse_traffic_signals.jsonl, urbanpulse_air_quality.jsonl, urbanpulse_smart_meters.jsonl")

# Execute the generator
generate_urbanpulse_streams(5000)
