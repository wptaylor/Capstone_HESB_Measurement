#!/usr/bin/env python3
"""
Dummy MLX90394 snapshot source.

It exposes an Arduino-like line protocol over TCP so that scan_mlx_z_stack.py can
use it as a stand-in for the real MLX board.

Protocol:
- start          : enable streaming snapshots
- stop           : disable streaming snapshots
- scan           : print a synthetic branch/sensor presence summary
- setz <mm>      : set the logical z position used by the field generator

Default behavior now populates all 112 possible sensors.
"""

from __future__ import annotations

import argparse
import math
import random
import socket
import sys
import time
from dataclasses import dataclass
from typing import Dict, List, Tuple

TCA1_CHANNELS = range(0, 7)
TCA2_CHANNELS = range(0, 8)
MLX_ADDRS = (0x60, 0x61)
TOTAL_SENSORS = 112


def decompose_index_1based(index_1based: int) -> Tuple[int, int, int]:
    idx0 = index_1based - 1
    tca1_ch = idx0 // 16
    rem = idx0 % 16
    tca2_ch = rem // 2
    addr = 0x60 if (rem % 2) == 0 else 0x61
    return tca1_ch, tca2_ch, addr


@dataclass
class DummySensor:
    index: int
    tca1: int
    tca2: int
    addr: int
    ax: float
    ay: float
    az: float
    phase_x: float
    phase_y: float
    phase_z: float
    z_gain_x: float
    z_gain_y: float
    z_gain_z: float
    temp_base: float


class DummyFieldTree:
    def __init__(self, populated_count: int, field_limit_uT: float, seed: int) -> None:
        rng = random.Random(seed)
        if populated_count >= TOTAL_SENSORS:
            chosen = list(range(1, TOTAL_SENSORS + 1))
        else:
            chosen = sorted(rng.sample(range(1, TOTAL_SENSORS + 1), populated_count))
        self.sensors: Dict[Tuple[int, int, int], DummySensor] = {}
        for idx in chosen:
            tca1, tca2, addr = decompose_index_1based(idx)
            self.sensors[(tca1, tca2, addr)] = DummySensor(
                index=idx,
                tca1=tca1,
                tca2=tca2,
                addr=addr,
                ax=rng.uniform(250.0, field_limit_uT),
                ay=rng.uniform(250.0, field_limit_uT),
                az=rng.uniform(250.0, field_limit_uT),
                phase_x=rng.uniform(0.0, 2.0 * math.pi),
                phase_y=rng.uniform(0.0, 2.0 * math.pi),
                phase_z=rng.uniform(0.0, 2.0 * math.pi),
                z_gain_x=rng.uniform(0.01, 0.06),
                z_gain_y=rng.uniform(0.01, 0.06),
                z_gain_z=rng.uniform(0.01, 0.06),
                temp_base=rng.uniform(22.0, 28.0),
            )
        self.field_limit_uT = field_limit_uT
        self.start_monotonic = time.monotonic()
        self.logical_z_mm = 0.0
        self.rng = rng

    def set_z_mm(self, z_mm: float) -> None:
        self.logical_z_mm = z_mm

    def sensor_cell_text(self, tca1: int, tca2: int, addr: int) -> str:
        sensor = self.sensors.get((tca1, tca2, addr))
        if sensor is None:
            return "ABSENT"
        t = time.monotonic() - self.start_monotonic
        z = self.logical_z_mm
        bx = sensor.ax * math.sin(sensor.phase_x + 0.55 * t + sensor.z_gain_x * z)
        by = sensor.ay * math.cos(sensor.phase_y + 0.47 * t + sensor.z_gain_y * z)
        bz = sensor.az * math.sin(sensor.phase_z + 0.63 * t + sensor.z_gain_z * z)
        bx = max(-self.field_limit_uT, min(self.field_limit_uT, bx))
        by = max(-self.field_limit_uT, min(self.field_limit_uT, by))
        bz = max(-self.field_limit_uT, min(self.field_limit_uT, bz))
        temp_c = sensor.temp_base + 0.6 * math.sin(0.15 * t + 0.02 * z + sensor.phase_x)
        age_ms = self.rng.randint(0, 220)
        flags = "-"
        return f" {bx:8.2f} {by:8.2f} {bz:8.2f} {temp_c:7.2f} {age_ms:6d} {flags:<6s}"

    def populated_branches(self) -> List[int]:
        return sorted({tca1 for (tca1, _tca2, _addr) in self.sensors.keys()})

    def scan_lines(self) -> List[str]:
        lines = ["Dummy I2C scan:"]
        for tca1 in self.populated_branches():
            lines.append(f"  TCA1 ch{tca1}: TCA2 0x77 present")
            for tca2 in TCA2_CHANNELS:
                addrs = []
                for addr in MLX_ADDRS:
                    if (tca1, tca2, addr) in self.sensors:
                        addrs.append(f"0x{addr:02X}")
                lines.append(f"    TCA2 ch{tca2}: {' '.join(addrs) if addrs else 'none'}")
        return lines

    def snapshot_lines(self, snapshot_ms: int) -> List[str]:
        lines: List[str] = []
        lines.append("")
        lines.append("##############################################################################################################")
        lines.append(f"Snapshot @ {snapshot_ms} ms   (units: uT, uT, uT, C, age_ms, flags)")
        branches = self.populated_branches()
        lines.append("Detected TCA2 branches with populated sensors: " + ", ".join(f"ch{b}" for b in branches))
        lines.append(f"Present MLX90394 devices: {len(self.sensors)}   Configured: {len(self.sensors)}")
        for tca1 in branches:
            lines.append("==============================================================================================================")
            lines.append(f"TCA1 ch{tca1}  ->  TCA2 0x77")
            lines.append("CH | Addr  |        X        Y        Z       T    Age Flags | Addr  |        X        Y        Z       T    Age Flags")
            lines.append("--------------------------------------------------------------------------------------------------------------")
            for tca2 in TCA2_CHANNELS:
                cell60 = self.sensor_cell_text(tca1, tca2, 0x60)
                cell61 = self.sensor_cell_text(tca1, tca2, 0x61)
                lines.append(f" {tca2} | 0x60 |{cell60} | 0x61 |{cell61}")
        lines.append("##############################################################################################################")
        return lines


class DummyServer:
    def __init__(self, host: str, port: int, snapshot_period_s: float, tree: DummyFieldTree) -> None:
        self.host = host
        self.port = port
        self.snapshot_period_s = snapshot_period_s
        self.tree = tree
        self.streaming = True
        self.client: socket.socket | None = None
        self.client_buffer = bytearray()
        self.start_monotonic = time.monotonic()
        self.last_snapshot_t = 0.0

    def send_lines(self, lines: List[str]) -> None:
        if self.client is None:
            return
        payload = ("\n".join(lines) + "\n").encode("utf-8")
        self.client.sendall(payload)

    def handle_command(self, line: str) -> None:
        cmd = line.strip()
        if not cmd:
            return
        if cmd == "start":
            self.streaming = True
            self.send_lines(["All detected sensors restarted in 5 Hz continuous mode."])
            return
        if cmd == "stop":
            self.streaming = False
            self.send_lines(["All configured sensors powered down."])
            return
        if cmd == "scan":
            self.send_lines(self.tree.scan_lines())
            return
        if cmd.startswith("setz "):
            try:
                z_mm = float(cmd.split(None, 1)[1])
            except Exception:
                self.send_lines(["ERROR: invalid setz value"])
                return
            self.tree.set_z_mm(z_mm)
            self.send_lines([f"Dummy logical z set to {z_mm:.6f} mm"])
            return
        self.send_lines([f"ERROR: unknown command: {cmd}"])

    def run(self) -> int:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as srv:
            srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            srv.bind((self.host, self.port))
            srv.listen(1)
            print(f"Dummy MLX source listening on tcp://{self.host}:{self.port}")
            print(f"Populated sensors: {len(self.tree.sensors)} / {TOTAL_SENSORS}")
            print("Commands: start | stop | scan | setz <mm>")
            while True:
                self.client, addr = srv.accept()
                self.client_buffer = bytearray()
                self.streaming = True
                self.last_snapshot_t = 0.0
                print(f"Client connected from {addr[0]}:{addr[1]}")
                self.client.settimeout(0.1)
                self.send_lines([
                    "Streaming dummy MLX90394 tree via TCP.",
                    "Commands: scan | stop | start | setz <mm>",
                ])
                while True:
                    now = time.monotonic()
                    if self.streaming and (now - self.last_snapshot_t) >= self.snapshot_period_s:
                        snapshot_ms = int((now - self.start_monotonic) * 1000.0)
                        self.send_lines(self.tree.snapshot_lines(snapshot_ms))
                        self.last_snapshot_t = now
                    try:
                        chunk = self.client.recv(4096)
                    except socket.timeout:
                        continue
                    if not chunk:
                        print("Client disconnected.")
                        try:
                            self.client.close()
                        except Exception:
                            pass
                        self.client = None
                        break
                    self.client_buffer.extend(chunk)
                    while b"\n" in self.client_buffer:
                        line, _, remainder = self.client_buffer.partition(b"\n")
                        self.client_buffer = bytearray(remainder)
                        self.handle_command(line.decode("utf-8", errors="replace").rstrip("\r"))


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run a dummy MLX snapshot source over TCP.")
    p.add_argument("--host", default="127.0.0.1", help="Listen host (default: 127.0.0.1)")
    p.add_argument("--port", type=int, default=8765, help="Listen TCP port (default: 8765)")
    p.add_argument("--snapshot-period", type=float, default=2.0, help="Seconds between snapshots while streaming")
    p.add_argument("--populated", type=int, default=112, help="Number of populated sensors out of 112 (default: 112)")
    p.add_argument("--field-limit-ut", type=float, default=2000.0, help="Absolute max field in uT")
    p.add_argument("--seed", type=int, default=394, help="Random seed for waveform parameters")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    if not (0 <= args.populated <= TOTAL_SENSORS):
        print("Error: --populated must be between 0 and 112", file=sys.stderr)
        return 2
    tree = DummyFieldTree(populated_count=args.populated, field_limit_uT=args.field_limit_ut, seed=args.seed)
    server = DummyServer(host=args.host, port=args.port, snapshot_period_s=args.snapshot_period, tree=tree)
    try:
        return server.run()
    except KeyboardInterrupt:
        print("Interrupted.")
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
