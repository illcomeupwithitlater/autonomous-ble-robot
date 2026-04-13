# Autonomous BLE Beacon Tracking Mobile Robot

## Overview
This is my Engineer's Thesis project from the Univesity. It is a fully autonomous, differential-drive mobile robot designed to track and follow a custom-built ESP32-C3 Bluetooth Low Energy (BLE) beacon.

<img width="1564" height="1116" alt="project_front" src="https://github.com/user-attachments/assets/15ddcebb-f36e-4ee8-bac9-c47066015a08" />
<img width="1724" height="1200" alt="robot_side" src="https://github.com/user-attachments/assets/8e34a455-c80d-43d8-8b46-161bc82977e8" />

## Technical Stack
* **Main Controller:** Raspberry Pi 5 (Python, `asyncio`, `threading`, `lgpio`)
* **Beacon Controller:** ESP32-C3/XIAO (Arduino/C++)
* **Hardware & Actuation:** TB6612FNG Dual Motor Driver, 3 USB-BT8500 BLE Adapters
* **Sensors:** RPLiDAR A1M8-R6, 3 HC-SR04 Ultrasonic Sensors
* **Design Tools:** KiCad (Schematics), AutoCAD (Mechanical Design)

## Core Features
* **Multithreaded State Management:** The robot runs on a concurrent architecture utilizing asynchronous BLE scanning (`BleakScanner`) alongside dedicated threads for LiDAR mapping, ultrasonic polling, a system watchdog, and the main navigation control loop.
* **Spatial Localization (BLE):** Actively scans for the target beacon using three distinct BLE adapters configured at specific angles. Calculates a weighted circular mean of RSSI values with exponential moving averages (EMA) to determine a tracking bearing.
* **Multi-Tier Obstacle Avoidance:**
  * *Long-Range Mapping:* LiDAR data is parsed into an angular bin array, evaluating clear paths using distance decay and clearance hysteresis.
  * *Short-Range/Blindspots:* A 3-sensor ultrasonic array acts as an emergency override, triggering immediate hard braking, reversing, and evasion maneuvers if an object breaches the safety threshold.
* **Kinematics & Motor Control:** The system utilizes a proportional (P) steering controller combined with a custom motor slew (ramping) algorithm to ensure smooth differential-drive acceleration and prevent voltage spikes on the TB6612FNG.
* **Telemetry & Data Logging:** Features an automated data-logging system that continuously writes timestamped state transitions (SEARCH, GO, AVOID, BLOCK), sensor minimums, and motor commands to a `.csv` file for post-run performance analysis and debugging.

## System Pinout
<img width="1038" height="650" alt="image" src="https://github.com/user-attachments/assets/7fdf22a8-d1ee-42bb-baaa-13c738e7fb0c" />
BLE modules and LiDAR were connected via USB.

## Mechanical Design
<img width="919" height="469" alt="image" src="https://github.com/user-attachments/assets/8db5502c-8741-428f-814e-eb5be32b7d8c" />

Casing for ESP32-C3

<img width="593" height="510" alt="image" src="https://github.com/user-attachments/assets/a96b8242-f4ec-45a0-8b8a-ec8956af35b1" />

Ultrasonic Sensor Holder

<img width="602" height="437" alt="image" src="https://github.com/user-attachments/assets/06952d0b-2ba9-4d73-b981-b8896910d165" />

BLE Sensor Stands

<img width="619" height="491" alt="image" src="https://github.com/user-attachments/assets/9bd39750-cd14-4345-bc29-ddeb2bf03994" />

LiDAR Stand

* Custom components and sensor mounts were designed in AutoCAD and 3D printed using resin printer. STL files are available in the `/mechanical` folder.
