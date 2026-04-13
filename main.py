import asyncio, csv, logging, math, statistics, threading, time, signal, sys, os, shutil, subprocess, re
from dataclasses import dataclass, field
from collections import deque
from enum import Enum, auto
from typing import Optional, Dict, Deque, List, Tuple

import RPi.GPIO as GPIO
import lgpio
from bleak import BleakScanner
from rplidar import RPLidar, RPLidarException


ROBOT_LEN_CM, ROBOT_WIDTH_CM, SAFETY_CM = 26, 16, 6
ROBOT_GROW_MM = int(round(math.hypot(ROBOT_LEN_CM/2, ROBOT_WIDTH_CM/2)*10 + SAFETY_CM*10))
LIDAR_BLOCK_MM, LIDAR_CLEAR_MM, LIDAR_SLOW_MM = ROBOT_GROW_MM+70, ROBOT_GROW_MM+60+70, 800
STALE_BLE, STALE_LIDAR, STALE_US = 2.0, 1.0, 0.6
WD_TIMEOUT_S = 1.5
MAX_HEADING_RATE_DPS = 220.0


ADAPTERS_BY_ROLE = {
    "left":  "08:BE:AC:46:C7:F6",
    "right": "08:BE:AC:46:C7:CB",
    "back":  "08:BE:AC:46:C7:F8",
}
ANGLES_BY_ROLE = {"left": 325, "right": 35, "back": 180}
SKIP_UART_BT = True
ADAPTERS: List[str] = []
ANGLES:  List[int] = []


TARGET_NAME_EXACT, TARGET_NAME_SUB = "XIAO-BEACON", "XIAO"
BEACON_MFG_ID, TARGET_MFG_SUBSTR = 18776, b"XIAO"
WINDOW, INTERVAL, DEBUG_BLE = 8, 0.30, False
RSSI_ALPHA, BLE_BEARING_ALPHA, RSSI_MAX_STEP_DB = 0.35, 0.25, 6.0


LIDAR_PORT, LIDAR_BAUDS = "/dev/ttyUSB0", [115200]
LIDAR_MAX_MM, LIDAR_SMOOTH_N = 8000, 7
LIDAR_IDLE_SLEEP, LIDAR_RETRY_SLEEP, LIDAR_FRONT_ARC = 0.03, 0.5, 30
LIDAR_YAW_DEG = 0.0
PLAN_HALF_ARC, PLAN_BINS, BIN_DECAY_MM_PER_S = 90, 31, 1200.0


SENSORS = {
    "front_left": {"TRIG": 22, "ECHO": 27},
    "front_center": {"TRIG": 23, "ECHO": 25},
    "front_right": {"TRIG": 24, "ECHO": 16},
}
SPEED_OF_SOUND_CM_S, US_TIMEOUT = 34300, 0.03
US_POLL_S, US_READS, US_MAX_CM = 0.10, 3, 200
US_BLOCK_CM, US_CLEAR_CM, US_SLOW_CM = 16, 22, 30
US_INTER_SENSOR_DELAY_S = 0.06
HARD_BRAKE_S, US_RELEASE_CLEAR_CNT, US_RELEASE_NOECHO_CNT = 0.30, 3, 3


PWMA, AIN1, AIN2 = 12, 5, 6
PWMB, BIN1, BIN2 = 13, 19, 26
STBY = 20
USE_RPI_PWM, PWM_FREQ = True, 1000
MOTOR_POLARITY = {"left": +1, "right": +1}
BASE_SPEED, MIN_SPEED, MAX_SPEED = 68, 30, 90

MOTOR_SLEW = 90.0
KICK_START_DUTY, KICK_START_MS = 58, 90

STEER_MAX, KP = 40.0, 40.0/90.0

MAX_REVERSE_S = 0.7
AVOID_HARD_TIMEOUT_S = 2.0

class Mode(Enum): SEARCH=auto(); GO=auto(); AVOID=auto()

def clamp(v, lo, hi): return max(lo, min(hi, v))
def rssi_weight(rssi, alpha=2.2): return 10 ** (rssi / (10 * alpha))

def circular_mean(angles_deg, weights):
    ang = [math.radians(a) for a in angles_deg]

    x = sum(w * math.cos(t) for w, t in zip(weights, ang))
    y = sum(w * math.sin(t) for w, t in zip(weights, ang))

    return (math.degrees(math.atan2(y, x)) + 360.0) % 360.0 if x or y else 0.0

def angle_wrap180(a): return (a + 180.0) % 360.0 - 180.0
def in_front_sector(angle_deg, center=0.0, arc=LIDAR_FRONT_ARC): return abs(angle_wrap180(angle_deg - center)) <= arc
def ema(prev, x, a): return x if prev is None else (a*x + (1.0-a)*prev)
def angle_ema(prev_angle, new_angle, alpha): return new_angle if prev_angle is None else circular_mean([prev_angle,new_angle],[1.0-alpha,alpha])


@dataclass
class Shared:
    lock: threading.Lock = field(default_factory=threading.Lock)
    running: bool = True

    rssi_bearing: Optional[float] = None
    lidar_clear: bool = True
    lidar_bins_mm: Optional[List[float]] = None
    lidar_bin_angles_deg: Optional[List[float]] = None

    us_min_cm: float = 999.0
    us_dists: Dict[str, float] = field(default_factory=lambda: {"front_left": 999.0, "front_center": 999.0, "front_right": 999.0})
    us_valids: Dict[str, bool] = field(default_factory=lambda: {"front_left": False, "front_center": False, "front_right": False})

    last_ble_ts: float = 0.0
    last_lidar_ts: float = 0.0
    last_us_ts: float = 0.0
    last_kick_ts: float = 0.0

    buffers: Dict[str, Deque[int]] = field(default_factory=dict)

shared = Shared()

def _parse_hciconfig():
    out = subprocess.check_output(["hciconfig"]).decode("utf-8","ignore")
    blocks = re.split(r'\n(?=hci\d+:)', out)
    name_by_addr, bus_by_name = {}, {}
    for blk in blocks:
        mname = re.search(r'^(hci\d+):', blk, re.M)
        if not mname: continue
        name = mname.group(1)
        maddr = re.search(r'BD Address:\s*([0-9A-F:]{17})', blk)
        mbus = re.search(r'Bus:\s*(\w+)', blk)
        addr = maddr.group(1).upper() if maddr else None
        bus = mbus.group(1).upper() if mbus else ""
        if addr: name_by_addr[addr] = name
        bus_by_name[name] = bus
    return name_by_addr, bus_by_name

def power_off_uart_controllers():
    if not shutil.which("btmgmt"): return
    try:
        _, bus_by_name = _parse_hciconfig()
        for hci, bus in bus_by_name.items():
            if bus == "UART":
                subprocess.run(["btmgmt","-i",hci,"power","off"],check=False,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    except Exception: pass

def resolve_adapters_from_macs(skip_uart=SKIP_UART_BT):
    name_by_addr, bus_by_name = _parse_hciconfig()
    ordered_hci, ordered_angles = [], []
    for role in ("left","right","back"):
        mac = ADAPTERS_BY_ROLE[role].upper()
        hci = name_by_addr.get(mac)
        if not hci: raise RuntimeError(f"No HCI for {role} ({mac})")
        if skip_uart and bus_by_name.get(hci) == "UART":
            raise RuntimeError(f"{hci} for {role} is UART; map to a USB dongle.")
        ordered_hci.append(hci); ordered_angles.append(ANGLES_BY_ROLE[role])
    return ordered_hci, ordered_angles

chip: Optional[int] = None
_last_ls=_last_rs=0.0; _last_t=time.monotonic(); _kick_until=0.0
_pwmA=_pwmB=None

def gpio_setup():
    global chip,_pwmA,_pwmB
    chip = lgpio.gpiochip_open(0)
    for pin in [AIN1,AIN2,BIN1,BIN2,STBY] + [v["TRIG"] for v in SENSORS.values()]:
        lgpio.gpio_claim_output(chip, pin)
    for v in SENSORS.values():
        lgpio.gpio_claim_input(chip, v["ECHO"])
    for v in SENSORS.values(): lgpio.gpio_write(chip, v["TRIG"], 0)
    time.sleep(0.2)
    lgpio.gpio_write(chip, STBY, 1)
    if USE_RPI_PWM:
        GPIO.setmode(GPIO.BCM); GPIO.setwarnings(False)
        GPIO.setup(PWMA, GPIO.OUT); GPIO.setup(PWMB, GPIO.OUT)
        _pwmA = GPIO.PWM(PWMA, PWM_FREQ); _pwmA.start(0)
        _pwmB = GPIO.PWM(PWMB, PWM_FREQ); _pwmB.start(0)

def _set_pwm(pin, duty_percent):
    duty = int(clamp(round(duty_percent), 0, 100))
    if USE_RPI_PWM:
        if pin==PWMA and _pwmA is not None: _pwmA.ChangeDutyCycle(duty); return
        if pin==PWMB and _pwmB is not None: _pwmB.ChangeDutyCycle(duty); return
    lgpio.gpio_write(chip, pin, 1 if duty>0 else 0)

def _write_dir(left:int, right:int):
    if left>0: lgpio.gpio_write(chip, AIN1,1); lgpio.gpio_write(chip, AIN2,0)
    elif left<0: lgpio.gpio_write(chip, AIN1,0); lgpio.gpio_write(chip, AIN2,1)
    else: lgpio.gpio_write(chip, AIN1,0); lgpio.gpio_write(chip, AIN2,0)
    if right>0: lgpio.gpio_write(chip, BIN1,1); lgpio.gpio_write(chip, BIN2,0)
    elif right<0: lgpio.gpio_write(chip, BIN1,0); lgpio.gpio_write(chip, BIN2,1)
    else: lgpio.gpio_write(chip, BIN1,0); lgpio.gpio_write(chip, BIN2,0)

def reset_ramp_state():
    global _last_ls,_last_rs,_kick_until
    _last_ls=_last_rs=0.0
    _kick_until=0.0

def drive(left_speed, right_speed):
    ls=int(clamp(left_speed*MOTOR_POLARITY["left"],-100,100))
    rs=int(clamp(right_speed*MOTOR_POLARITY["right"],-100,100))
    _write_dir(ls, rs); _set_pwm(PWMA, abs(ls)); _set_pwm(PWMB, abs(rs))

def drive_ramped(left_speed,right_speed):
    global _last_ls,_last_rs,_last_t,_kick_until
    now=time.monotonic(); dt=max(1e-3, now-_last_t); _last_t=now
    tl=clamp(left_speed,-100,100); tr=clamp(right_speed,-100,100)
    if tl!=0 and _last_ls*tl<0:
        _last_ls=0.0; _kick_until = max(_kick_until, now + KICK_START_MS/1000.0)
    if tr!=0 and _last_rs*tr<0:
        _last_rs=0.0; _kick_until = max(_kick_until, now + KICK_START_MS/1000.0)
    was_stopped=(abs(_last_ls)<5.0 and abs(_last_rs)<5.0)
    small_cmd=(max(abs(tl),abs(tr))>0) and (max(abs(tl),abs(tr))<KICK_START_DUTY)
    if was_stopped and small_cmd and now>_kick_until: _kick_until=now+KICK_START_MS/1000.0
    if now<_kick_until:
        kl=0.0 if abs(tl)<1e-3 else math.copysign(KICK_START_DUTY,tl)
        kr=0.0 if abs(tr)<1e-3 else math.copysign(KICK_START_DUTY,tr)
        drive(kl,kr); _last_ls,_last_rs=kl,kr; return
    step=MOTOR_SLEW*dt
    def slew(target,current):
        if target>current: return min(target,current+step)
        if target<current: return max(target,current-step)
        return current
    ls=slew(tl,_last_ls); rs=slew(tr,_last_rs)
    def apply_min(cmd): return 0.0 if abs(cmd)<1e-3 else math.copysign(max(abs(cmd),MIN_SPEED),cmd)
    ls_cmd=apply_min(ls) if abs(ls)>=1e-3 else 0.0
    rs_cmd=apply_min(rs) if abs(rs)>=1e-3 else 0.0
    ls_cmd=clamp(ls_cmd,-MAX_SPEED,MAX_SPEED); rs_cmd=clamp(rs_cmd,-MAX_SPEED,MAX_SPEED)
    drive(ls_cmd,rs_cmd); _last_ls,_last_rs=ls_cmd,rs_cmd

def stop(): drive(0,0)
def brake_hard():
    reset_ramp_state()
    drive(0,0)

def read_distance_cm(trig, echo, timeout=US_TIMEOUT):
    lgpio.gpio_write(chip,trig,0); time.sleep(0.00002)
    lgpio.gpio_write(chip,trig,1); time.sleep(0.00001)
    lgpio.gpio_write(chip,trig,0)
    t0=time.monotonic()
    while lgpio.gpio_read(chip,echo)==0:
        if time.monotonic()-t0>timeout: return None
    start=time.monotonic()
    while lgpio.gpio_read(chip,echo)==1:
        if time.monotonic()-start>timeout: return None
    return (time.monotonic()-start)*SPEED_OF_SOUND_CM_S/2.0

def read_distance_median(trig, echo):
    vals=[]
    for _ in range(US_READS):
        d=read_distance_cm(trig,echo)
        if d is not None and 1.0<=d<=US_MAX_CM: vals.append(d)
        time.sleep(0.008)
    return statistics.median(vals) if vals else None

def ultrasound_loop(log):
    order=["front_left","front_center","front_right"]
    while True:
        with shared.lock:
            if not shared.running: break
        dists, valids = {}, {}
        for name in order:
            pins=SENSORS[name]; d=read_distance_median(pins["TRIG"],pins["ECHO"])
            if d is None: valids[name]=False; d_effective=999.0
            else: valids[name]=True; d_effective=d
            dists[name]=d_effective
            time.sleep(US_INTER_SENSOR_DELAY_S)
        with shared.lock:
            shared.us_dists.update(dists); shared.us_valids.update(valids)
            shared.us_min_cm=min(dists.values()) if dists else 999.0
            shared.last_us_ts=time.monotonic()
        time.sleep(US_POLL_S)

def _hysteresis_update(prev_clear,min_front_mm):
    return not (min_front_mm<=LIDAR_BLOCK_MM) if prev_clear else (min_front_mm>=LIDAR_CLEAR_MM)
def make_plan_bins(): return [(-PLAN_HALF_ARC + i*(2*PLAN_HALF_ARC)/(PLAN_BINS-1)) for i in range(PLAN_BINS)]

def lidar_loop_robust(log):
    current_baud_idx=0; lidar=None
    front_hist=deque(maxlen=LIDAR_SMOOTH_N); prev_clear=True
    bin_angles=make_plan_bins(); prev_bins=None; prev_time=time.monotonic()
    rot=lambda a_raw: angle_wrap180(a_raw - LIDAR_YAW_DEG)
    while True:
        with shared.lock:
            if not shared.running: break
        try:
            baud=LIDAR_BAUDS[current_baud_idx]
            lidar=RPLidar(LIDAR_PORT,baudrate=baud,timeout=3); lidar.start_motor()
            try: logging.getLogger("LiDAR").info("LiDAR %d baud | Info=%s | Health=%s", baud, lidar.get_info(), lidar.get_health())
            except Exception: pass
            try: lidar.clean_input()
            except Exception: pass
            for scan in lidar.iter_scans():
                with shared.lock:
                    if not shared.running: break
                front_mm=[dist for (_,ang_raw,dist) in scan if in_front_sector(rot(ang_raw)) and 0<dist<=LIDAR_MAX_MM]
                if front_mm:
                    front_hist.append(min(front_mm)); sm=statistics.median(front_hist)
                    prev_clear=_hysteresis_update(prev_clear, sm)
                    with shared.lock: shared.lidar_clear=prev_clear
                bins=[LIDAR_MAX_MM+1]*PLAN_BINS
                for (_, ang_raw,dist) in scan:
                    if not (0<dist<=LIDAR_MAX_MM): continue
                    a=rot(ang_raw)
                    if abs(a)>PLAN_HALF_ARC: continue
                    idx=round((a+PLAN_HALF_ARC)/(2*PLAN_HALF_ARC)*(PLAN_BINS-1))
                    if 0<=idx<PLAN_BINS: bins[idx]=min(bins[idx],dist)
                now=time.monotonic(); dt=max(1e-3, now-prev_time)
                if prev_bins is None: decayed=bins
                else:
                    inc=BIN_DECAY_MM_PER_S*dt
                    decayed=[nb if nb<pb else min(nb, pb+inc) for nb,pb in zip(bins,prev_bins)]
                prev_bins,prev_time=decayed,now
                with shared.lock:
                    shared.lidar_bins_mm=decayed; shared.lidar_bin_angles_deg=bin_angles; shared.last_lidar_ts=now
                time.sleep(LIDAR_IDLE_SLEEP)
        except RPLidarException as e:
            logging.getLogger("LiDAR").warning("LiDAR scan error: %s. Resetting.", e)
            try:
                if lidar: lidar.stop()
            except Exception: ...
            try:
                if lidar: lidar.clean_input()
            except Exception: ...
            time.sleep(LIDAR_RETRY_SLEEP)
            if "wrong" in str(e).lower(): current_baud_idx=(current_baud_idx+1)%len(LIDAR_BAUDS)
        except Exception as e:
            logging.getLogger("LiDAR").error("LiDAR unexpected: %s. Reinit in %.1fs", e, LIDAR_RETRY_SLEEP); time.sleep(LIDAR_RETRY_SLEEP)
        finally:
            if lidar:
                try: lidar.stop()
                except Exception: ...
                try: lidar.stop_motor()
                except Exception: ...
                try: lidar.disconnect()
                except Exception: ...
                lidar=None

def beacon_match(device, adv_data):
    if TARGET_NAME_EXACT and ((device.name==TARGET_NAME_EXACT) or (adv_data.local_name==TARGET_NAME_EXACT)): return True
    if TARGET_NAME_SUB and ((device.name and TARGET_NAME_SUB in device.name) or (adv_data.local_name and TARGET_NAME_SUB in adv_data.local_name)): return True
    mfg=adv_data.manufacturer_data or {}
    for k,v in mfg.items():
        if (BEACON_MFG_ID is not None) and (k==BEACON_MFG_ID): return True
        if isinstance(v,(bytes,bytearray)) and TARGET_MFG_SUBSTR in v: return True
    return False

def reset_adapter_powercycle():
    if shutil.which("btmgmt"):
        try:
            subprocess.run(["btmgmt","power","off"],check=False,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
            subprocess.run(["btmgmt","power","on"], check=False,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        except Exception: pass

async def scan_once_on_adapter(adapter, log, rssi_ema, last_med):
    def cb(device, adv_data):
        rssi=getattr(device,"rssi",None) or getattr(adv_data,"rssi",None)
        if DEBUG_BLE: log.debug("[%s] seen: %s name=%r local=%r rssi=%s mfg=%s", adapter, device.address, device.name, adv_data.local_name, rssi, adv_data.manufacturer_data)
        if rssi is None: return
        if beacon_match(device, adv_data): shared.buffers[adapter].append(rssi)
    scanner=None
    try:
        scanner=BleakScanner(adapter=adapter, detection_callback=cb); await scanner.start(); await asyncio.sleep(INTERVAL)
    except Exception as e:
        log.warning("[%s] scanner error: %s", adapter, e); reset_adapter_powercycle()
    finally:
        if scanner:
            try: await scanner.stop()
            except Exception: pass
    if len(shared.buffers[adapter])>=3:
        med=statistics.median(shared.buffers[adapter])
        prev=last_med.get(adapter)
        med_limited=med if prev is None else (prev + clamp(med-prev,-RSSI_MAX_STEP_DB,RSSI_MAX_STEP_DB))
        last_med[adapter]=med_limited
        rssi_ema[adapter]=ema(rssi_ema.get(adapter), med_limited, RSSI_ALPHA)

async def ble_rssi_loop(log):
    log.info("Starting BLE RSSI loop.")
    rssi_ema={a:None for a in ADAPTERS}; last_med={a:None for a in ADAPTERS}
    while True:
        with shared.lock:
            if not shared.running: break
        await asyncio.gather(*(scan_once_on_adapter(a,log,rssi_ema,last_med) for a in ADAPTERS))
        vals=[rssi_ema.get(a) for a in ADAPTERS]
        if all(v is not None for v in vals):
            weights=[rssi_weight(v) for v in vals]; bearing=circular_mean(ANGLES, weights); now=time.monotonic()
            with shared.lock:
                prev=shared.rssi_bearing
                shared.rssi_bearing=angle_ema(prev, bearing, BLE_BEARING_ALPHA)
                shared.last_ble_ts=now; out=shared.rssi_bearing
            log.info("RSSI %s -> Bearing %6.1f°", dict(zip(ADAPTERS,(round(v,1) for v in vals))), out if out is not None else -1)
        await asyncio.sleep(0.05)

def pick_heading(goal_deg, bin_angles, bins_mm, stop_mm, slow_mm):
    best=None
    for ang,dist in zip(bin_angles,bins_mm):
        d=clamp(dist,0,LIDAR_MAX_MM)
        clear_term=0.0 if d<=stop_mm else 1.0 if d>=slow_mm else (d-stop_mm)/(slow_mm-stop_mm)
        goal_err=abs(angle_wrap180(ang-goal_deg)); goal_term=clamp(1.0-(goal_err/90.0),0.0,1.0)
        score=0.65*clear_term + 0.35*goal_term
        tup=(score,ang,d); best=tup if (best is None or tup>best) else best
    _,best_ang,best_dist = best if best else (0.0,0.0,0.0)
    return best_ang,best_dist

_last_heading_cmd=0.0; _last_heading_ts=time.monotonic()
def limit_heading_slew(target_deg):
    global _last_heading_cmd,_last_heading_ts
    now=time.monotonic(); dt=max(1e-3, now-_last_heading_ts); _last_heading_ts=now
    delta=angle_wrap180(target_deg - _last_heading_cmd); max_delta=MAX_HEADING_RATE_DPS*dt
    if abs(delta)>max_delta: delta=math.copysign(max_delta, delta)
    _last_heading_cmd=angle_wrap180(_last_heading_cmd + delta); return _last_heading_cmd

def watchdog_loop(log):
    while True:
        with shared.lock:
            if not shared.running: break
            last=shared.last_kick_ts
        if time.monotonic()-last>WD_TIMEOUT_S:
            log.error("Watchdog timeout!")
            try: stop(); lgpio.gpio_write(chip, STBY, 0)
            except Exception: pass
        time.sleep(0.25)

def control_loop(log, csv_writer):
    log.info("Main control loop running.")

    us_blocked = False
    clear_cnt = 0
    noecho_cnt = 0
    avoid_spin_dir = +1
    brake_until = 0.0
    back_until = 0.0
    avoid_enter_ts = 0.0

    while True:
        with shared.lock:
            if not shared.running:
                break

            now = time.monotonic()
            shared.last_kick_ts = now

            ble_stale   = (now - shared.last_ble_ts   > STALE_BLE)
            lidar_alive = (now - shared.last_lidar_ts <= STALE_LIDAR)
            us_alive    = (now - shared.last_us_ts    <= STALE_US)

            us_d   = dict(shared.us_dists)
            us_v   = dict(shared.us_valids)
            us_min = min(us_d.values()) if us_d else 999.0

            lidar_clear = shared.lidar_clear
            bins        = shared.lidar_bins_mm
            bin_angles  = shared.lidar_bin_angles_deg

            goal = None if ble_stale else shared.rssi_bearing

        if not lidar_alive and not us_alive:
            stop()
            try:
                lgpio.gpio_write(chip, STBY, 0)
            except Exception:
                pass
            csv_writer.writerow([now, "STALE-BOTH", 0, 0, us_min, lidar_clear,
                                 "", "", "", ""])
            logging.getLogger("CTRL").error(
                "Both LiDAR and ultrasound stale – stopping for safety."
            )
            time.sleep(0.05)
            continue

        if not us_alive:
            if us_blocked:
                logging.getLogger("CTRL").warning(
                    "US stale while us_blocked=True – forcing unblock."
                )
            us_blocked = False
            clear_cnt = 0
            noecho_cnt = 0
            us_d = {}
            us_v = {"front_left": False,
                    "front_center": False,
                    "front_right": False}
            us_min = 999.0

        if not lidar_alive:
            lidar_clear = True
            bins = None
            bin_angles = None

        try:
            lgpio.gpio_write(chip, STBY, 1)
        except Exception:
            pass

        any_valid_close = any(
            us_v.get(k, False) and us_d.get(k, 999.0) <= US_BLOCK_CM
            for k in ("front_left", "front_center", "front_right")
        )
        all_valid_and_clear = all(
            (not us_v.get(k, False)) or (us_d.get(k, 999.0) >= US_CLEAR_CM)
            for k in ("front_left", "front_center", "front_right")
        )
        all_invalid = all(
            not us_v.get(k, False)
            for k in ("front_left", "front_center", "front_right")
        )
        center_clear = us_v.get("front_center", False) and (
            us_d.get("front_center", 999.0) >= US_CLEAR_CM
        )

        best_ang = best_dist = None
        if (bins is not None) and (bin_angles is not None):
            goal_deg = goal if (goal is not None) else 0.0
            ba, bd = pick_heading(
                goal_deg, bin_angles, bins, LIDAR_BLOCK_MM, LIDAR_SLOW_MM
            )
            best_ang, best_dist = ba, bd

        if us_blocked and all_valid_and_clear:
            brake_until = 0.0
            back_until = 0.0

        if any_valid_close and not us_blocked:
            us_blocked = True
            clear_cnt = 0
            noecho_cnt = 0
            avoid_enter_ts = now

            ld = us_d.get("front_left", 999.0)
            rd = us_d.get("front_right", 999.0)
            lv = us_v.get("front_left", False)
            rv = us_v.get("front_right", False)

            if lv and rv:
                avoid_spin_dir = +1 if ld <= rd else -1
            elif lv and not rv:
                avoid_spin_dir = +1
            elif rv and not lv:
                avoid_spin_dir = -1
            else:
                avoid_spin_dir = +1

            brake_until = now + HARD_BRAKE_S
            back_until = brake_until + min(0.35, MAX_REVERSE_S)
            brake_hard()

        elif us_blocked:
            clear_cnt = clear_cnt + 1 if all_valid_and_clear else 0
            noecho_cnt = noecho_cnt + 1 if all_invalid else 0

            if (clear_cnt >= US_RELEASE_CLEAR_CNT or
                noecho_cnt >= US_RELEASE_NOECHO_CNT or
                center_clear):
                us_blocked = False
                clear_cnt = 0
                noecho_cnt = 0
                with shared.lock:
                    shared.lidar_clear = True
                reset_ramp_state()

        if us_blocked:
            logging.getLogger("CTRL").info(
                "US %s %s %s L=%.1f C=%.1f R=%.1f | min=%.1f | blocked=True",
                "[v]" if us_v.get("front_left", False) else "[-]",
                "[v]" if us_v.get("front_center", False) else "[-]",
                "[v]" if us_v.get("front_right", False) else "[-]",
                us_d.get("front_left", 999.0),
                us_d.get("front_center", 999.0),
                us_d.get("front_right", 999.0),
                us_min,
            )

            if (best_ang is not None) and (best_dist is not None):
                if (best_dist >= LIDAR_CLEAR_MM) and (abs(best_ang) >= 35.0):
                    us_blocked = False
                    clear_cnt = 0
                    noecho_cnt = 0
                    with shared.lock:
                        shared.lidar_clear = True
                    reset_ramp_state()
                    continue

            if (now - avoid_enter_ts) > AVOID_HARD_TIMEOUT_S:
                us_blocked = False
                clear_cnt = 0
                noecho_cnt = 0
                with shared.lock:
                    shared.lidar_clear = True
                reset_ramp_state()
                continue

            if now < brake_until:
                brake_hard()
                csv_writer.writerow(
                    [now, "AVOID-BRAKE", 0, 0, us_min, lidar_clear,
                     "", "", "", ""]
                )
                time.sleep(0.05)
                continue

            elif now < back_until:
                if avoid_spin_dir > 0:
                    cmdL, cmdR = -70, -40
                else:
                    cmdL, cmdR = -40, -70

            else:
                cmdL, cmdR = (-60, 60) if avoid_spin_dir > 0 else (60, -60)

            drive_ramped(cmdL, cmdR)
            csv_writer.writerow(
                [now, "AVOID", cmdL, cmdR, us_min, lidar_clear,
                 "", "", "", ""]
            )
            time.sleep(0.05)
            continue

        if not lidar_clear:
            cmdL, cmdR = -50, -50
            drive_ramped(cmdL, cmdR)
            csv_writer.writerow(
                [now, "AVOID-LDR", cmdL, cmdR, us_min, lidar_clear,
                 "", "", "", ""]
            )
            time.sleep(0.05)
            continue

        if (bins is not None) and (bin_angles is not None):
            goal_deg = goal if (goal is not None) else 0.0
            best_ang, best_dist = pick_heading(
                goal_deg, bin_angles, bins, LIDAR_BLOCK_MM, LIDAR_SLOW_MM
            )

            if best_dist <= LIDAR_BLOCK_MM:
                drive_ramped(-50, -50)
                csv_writer.writerow(
                    [now, "BLOCK", -50, -50, us_min, lidar_clear,
                     best_ang, best_dist, goal_deg, ""]
                )
                time.sleep(0.05)
                continue

            limited = limit_heading_slew(best_ang)
            err = angle_wrap180(limited)
            turn = clamp(KP * (-err), -STEER_MAX, STEER_MAX)

            lidar_scale = (
                1.0
                if best_dist >= LIDAR_SLOW_MM
                else clamp(
                    (best_dist - LIDAR_BLOCK_MM)
                    / (LIDAR_SLOW_MM - LIDAR_BLOCK_MM),
                    0.45,
                    1.0,
                )
            )

            if any(
                us_v.get(k, False) and us_d.get(k, 999.0) < US_SLOW_CM
                for k in ("front_left", "front_center", "front_right")
            ):
                us_near = min(
                    us_d[k]
                    for k in ("front_left", "front_center", "front_right")
                    if us_v.get(k, False)
                )
                us_scale = clamp(
                    (us_near - US_BLOCK_CM)
                    / max(1.0, (US_SLOW_CM - US_BLOCK_CM)),
                    0.35,
                    0.75,
                )
            else:
                us_scale = 1.0

            base = clamp(
                BASE_SPEED * lidar_scale * us_scale,
                MIN_SPEED,
                MAX_SPEED,
            )
            left = clamp(base - turn, MIN_SPEED, MAX_SPEED)
            right = clamp(base + turn, MIN_SPEED, MAX_SPEED)

            drive_ramped(left, right)
            csv_writer.writerow(
                [now, "GO", left, right, us_min, lidar_clear,
                 best_ang, best_dist, goal_deg, limited]
            )

        else:
            if goal is not None:
                limited = limit_heading_slew(goal)
                err = angle_wrap180(limited)
                turn = clamp(KP * (-err), -STEER_MAX, STEER_MAX)

                base = BASE_SPEED
                left = clamp(base - turn, MIN_SPEED, MAX_SPEED)
                right = clamp(base + turn, MIN_SPEED, MAX_SPEED)

                drive_ramped(left, right)
                csv_writer.writerow(
                    [now, "GO-NOBINS", left, right, us_min, lidar_clear,
                     "", "", "", limited]
                )
            else:
                drive_ramped(45, 30)
                csv_writer.writerow(
                    [now, "SEARCH", 45, 30, us_min, lidar_clear,
                     "", "", "", ""]
                )

        time.sleep(0.05)


def shutdown(*_):
    with shared.lock:
        if not shared.running: return
        shared.running=False
    logging.getLogger("SYS").info("Shutting down.")
    try: stop()
    except Exception: pass
    try:
        if USE_RPI_PWM and _pwmA is not None: _pwmA.stop()
        if USE_RPI_PWM and _pwmB is not None: _pwmB.stop()
        if USE_RPI_PWM: GPIO.cleanup([PWMA,PWMB])
    except Exception: pass
    try:
        if chip is not None: lgpio.gpio_write(chip, STBY, 0)
    except Exception: pass
    try:
        if chip is not None: lgpio.gpiochip_close(chip)
    except Exception: pass

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    log_ble=logging.getLogger("BLE"); log_lidar=logging.getLogger("LiDAR"); log_ctrl=logging.getLogger("CTRL")
    log_us=logging.getLogger("US"); log_sys=logging.getLogger("SYS"); log_wd=logging.getLogger("WD")
    gpio_setup()
    log_sys.info("GPIO OK. STBY=1. Grow=%d | Block=%d | Clear=%d | Slow=%d", ROBOT_GROW_MM, LIDAR_BLOCK_MM, LIDAR_CLEAR_MM, LIDAR_SLOW_MM)
    power_off_uart_controllers()
    adapters,angles=resolve_adapters_from_macs(); globals()["ADAPTERS"]=adapters; globals()["ANGLES"]=angles
    with shared.lock:
        shared.buffers={a:deque(maxlen=WINDOW) for a in ADAPTERS}; shared.last_kick_ts=time.monotonic()
    log_sys.info("BLE adapters: left=%s right=%s back=%s | angles=%s", ADAPTERS[0],ADAPTERS[1],ADAPTERS[2],ANGLES)
    os.makedirs("logs", exist_ok=True)
    fname=time.strftime("logs/run_%Y%m%d_%H%M%S.csv"); f=open(fname,"w",newline=""); csv_writer=csv.writer(f)
    csv_writer.writerow(["t","mode","cmdL","cmdR","us_min_cm","lidar_front_clear","best_ang_deg","best_dist_mm","goal_deg","heading_cmd_deg"])
    t_ultra=threading.Thread(target=ultrasound_loop,args=(log_us,),daemon=True)
    t_lidar=threading.Thread(target=lidar_loop_robust,args=(log_lidar,),daemon=True)
    t_ctrl=threading.Thread(target=control_loop,args=(log_ctrl,csv_writer),daemon=True)
    t_wd=threading.Thread(target=watchdog_loop,args=(log_wd,),daemon=True)
    t_ultra.start(); t_lidar.start(); t_ctrl.start(); t_wd.start()
    try: asyncio.run(ble_rssi_loop(log_ble))
    except KeyboardInterrupt: pass
    finally:
        shutdown()
        for t in (t_ultra,t_lidar,t_ctrl,t_wd): t.join(timeout=1.0)
        try: f.flush(); f.close()
        except Exception: pass
        log_sys.info("Exited. Log: %s", fname)

if __name__=="__main__":
    for s in (signal.SIGINT, signal.SIGTERM):
        try: signal.signal(s, shutdown)
        except Exception: pass
    main()
