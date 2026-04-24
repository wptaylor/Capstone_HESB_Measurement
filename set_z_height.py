#!/usr/bin/env python3
"""
Set the PCB z-height by commanding a stepper-controller Arduino over serial.

Assumptions:
- The Arduino is running nema_step_control.ino or a protocol-compatible sketch.
- The Arduino accepts a signed integer step count over serial and replies with
  lines including:
      Moving <N> steps
      Done. Enter next value:
- The stepper stage is open-loop. Absolute position is therefore tracked in a
  local JSON state file on the PC.
- 1 motor step = 1.5 um of z travel.

Typical workflow:
    1) Manually place the stage at the mechanical/electrical 0-step reference.
    2) Run:  py set_z_height.py --zero-here
    3) Move to an absolute z height:
            py set_z_height.py --port COM8 --target-mm 12.0

Requirements:
    py -m pip install pyserial
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import pathlib
import sys
import time
from dataclasses import dataclass
from typing import Any

try:
    import serial
    from serial.tools import list_ports
except ImportError as exc:
    raise SystemExit(
        "Missing dependency: pyserial\n"
        "Install with: py -m pip install pyserial"
    ) from exc


SCRIPT_DIR = pathlib.Path(__file__).resolve().parent
DEFAULT_STATE_PATH = SCRIPT_DIR / "z_stage_state.json"
STEP_SIZE_UM = 1.5
# User-facing +z is opposite the Arduino motor-step sign on this stage.
# Set to +1 if a positive commanded step should increase z, or -1 to invert it.
STAGE_DIRECTION_SIGN = 1
DEFAULT_BAUD = 115200


class StageProtocolError(RuntimeError):
    """Raised when the Arduino serial protocol does not behave as expected."""


@dataclass
class StageState:
    current_steps: int = 0
    step_size_um: float = STEP_SIZE_UM
    updated_at: str | None = None
    note: str = ""

    @property
    def current_um(self) -> float:
        return self.current_steps * self.step_size_um

    @property
    def current_mm(self) -> float:
        return self.current_um / 1000.0


@dataclass
class MoveResult:
    requested_delta_steps: int
    confirmed_delta_steps: int | None
    lines: list[str]
    duration_s: float


@dataclass
class ActionSummary:
    action: str
    state_file: pathlib.Path
    current_steps_before: int
    current_steps_after: int
    target_steps: int | None
    target_um: float | None
    moved_steps: int
    port: str | None
    baud: int
    timestamp_local: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "action": self.action,
            "state_file": str(self.state_file),
            "current_steps_before": self.current_steps_before,
            "current_steps_after": self.current_steps_after,
            "target_steps": self.target_steps,
            "target_um": self.target_um,
            "target_mm": None if self.target_um is None else self.target_um / 1000.0,
            "moved_steps": self.moved_steps,
            "port": self.port,
            "baud": self.baud,
            "timestamp_local": self.timestamp_local,
            "step_size_um": STEP_SIZE_UM,
            "stage_direction_sign": STAGE_DIRECTION_SIGN,
        }


def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")


def local_now_iso() -> str:
    return dt.datetime.now().astimezone().isoformat(timespec="seconds")


def atomic_write_json(path: pathlib.Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def load_state(path: pathlib.Path) -> StageState:
    if not path.exists():
        return StageState(updated_at=utc_now_iso(), note="initialized")

    data = json.loads(path.read_text(encoding="utf-8"))
    current_steps = int(data.get("current_steps", 0))
    step_size_um = float(data.get("step_size_um", STEP_SIZE_UM))
    if abs(step_size_um - STEP_SIZE_UM) > 1e-9:
        raise ValueError(
            f"State file step_size_um={step_size_um} disagrees with script constant {STEP_SIZE_UM}"
        )
    return StageState(
        current_steps=current_steps,
        step_size_um=step_size_um,
        updated_at=data.get("updated_at"),
        note=str(data.get("note", "")),
    )


def save_state(path: pathlib.Path, state: StageState, note: str = "") -> None:
    payload = {
        "current_steps": int(state.current_steps),
        "step_size_um": float(state.step_size_um),
        "current_um": state.current_um,
        "current_mm": state.current_mm,
        "updated_at": utc_now_iso(),
        "note": note,
        "stage_direction_sign": STAGE_DIRECTION_SIGN,
    }
    atomic_write_json(path, payload)


def steps_from_target_um(target_um: float) -> int:
    return int(round(target_um / STEP_SIZE_UM))


def target_um_from_steps(steps: int) -> float:
    return steps * STEP_SIZE_UM


def list_serial_ports() -> None:
    ports = list(list_ports.comports())
    if not ports:
        print("No serial ports found.")
        return
    print("Available serial ports:")
    for p in ports:
        desc = f" - {p.description}" if p.description else ""
        print(f"  {p.device}{desc}")


def open_serial_port(port: str, baud: int, settle_s: float) -> serial.Serial:
    ser = serial.Serial(port=port, baudrate=baud, timeout=0.25, write_timeout=2)
    time.sleep(settle_s)
    ser.reset_input_buffer()
    ser.reset_output_buffer()
    return ser


def drain_serial(ser: serial.Serial, duration_s: float) -> list[str]:
    deadline = time.monotonic() + max(0.0, duration_s)
    lines: list[str] = []
    while time.monotonic() < deadline:
        raw = ser.readline()
        if not raw:
            continue
        lines.append(raw.decode("utf-8", errors="replace").rstrip("\r\n"))
    return lines


def send_relative_steps_and_wait(
    ser: serial.Serial,
    delta_steps: int,
    timeout_s: float,
) -> MoveResult:
    device_delta_steps = int(STAGE_DIRECTION_SIGN * delta_steps)
    payload = f"{device_delta_steps}\n".encode("utf-8")
    n = ser.write(payload)
    ser.flush()
    if n != len(payload):
        raise StageProtocolError("Failed to write full step command to the Arduino")

    deadline = time.monotonic() + timeout_s
    t0 = time.monotonic()
    lines: list[str] = []
    confirmed_delta_steps: int | None = None

    while time.monotonic() < deadline:
        raw = ser.readline()
        if not raw:
            continue
        line = raw.decode("utf-8", errors="replace").rstrip("\r\n")
        lines.append(line)

        if line.startswith("Moving ") and line.endswith(" steps"):
            middle = line[len("Moving ") : -len(" steps")]
            try:
                confirmed_delta_steps = int(middle.strip())
            except ValueError:
                pass

        if line.startswith("Done"):
            return MoveResult(
                requested_delta_steps=delta_steps,
                confirmed_delta_steps=confirmed_delta_steps,
                lines=lines,
                duration_s=time.monotonic() - t0,
            )

    raise TimeoutError(
        f"Timed out waiting for Arduino motion completion on {ser.port} after {timeout_s:.1f} s"
    )


def build_summary(
    *,
    action: str,
    state_path: pathlib.Path,
    current_steps_before: int,
    current_steps_after: int,
    target_steps: int | None,
    port: str | None,
    baud: int,
) -> ActionSummary:
    return ActionSummary(
        action=action,
        state_file=state_path.resolve(),
        current_steps_before=current_steps_before,
        current_steps_after=current_steps_after,
        target_steps=target_steps,
        target_um=None if target_steps is None else target_um_from_steps(target_steps),
        moved_steps=current_steps_after - current_steps_before,
        port=port,
        baud=baud,
        timestamp_local=local_now_iso(),
    )


def print_human_summary(summary: ActionSummary) -> None:
    print(f"Action: {summary.action}")
    print(f"State file: {summary.state_file}")
    if summary.port:
        print(f"Port: {summary.port} @ {summary.baud}")
    print(f"Position before: {summary.current_steps_before} steps ({summary.current_steps_before * STEP_SIZE_UM:.1f} um)")
    print(f"Position after:  {summary.current_steps_after} steps ({summary.current_steps_after * STEP_SIZE_UM:.1f} um)")
    if summary.target_steps is not None:
        print(f"Target:          {summary.target_steps} steps ({summary.target_um:.1f} um)")
    print(f"Delta moved:     {summary.moved_steps} steps ({summary.moved_steps * STEP_SIZE_UM:.1f} um)")
    print(f"Stage dir sign:  {STAGE_DIRECTION_SIGN:+d} (device steps per +z step)")
    print(f"Time:            {summary.timestamp_local}")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Set an open-loop z stage to an absolute height referenced to the 0-step position. "
            "Absolute position is tracked in a local JSON state file."
        )
    )
    p.add_argument("--port", help="Serial port for the stepper Arduino, e.g. COM8")
    p.add_argument("--baud", type=int, default=DEFAULT_BAUD, help=f"Baud rate (default: {DEFAULT_BAUD})")
    p.add_argument(
        "--settle",
        type=float,
        default=2.5,
        help="Seconds to wait after opening the serial port before sending commands (default: 2.5)",
    )
    p.add_argument(
        "--timeout",
        type=float,
        default=60.0,
        help="Seconds to wait for motion completion after sending steps (default: 60)",
    )
    p.add_argument(
        "--startup-read",
        type=float,
        default=0.8,
        help="Seconds to read and discard startup/banner text after opening serial (default: 0.8)",
    )
    p.add_argument(
        "--state-file",
        type=pathlib.Path,
        default=DEFAULT_STATE_PATH,
        help=f"JSON file storing the host-side absolute step position (default: {DEFAULT_STATE_PATH.name})",
    )
    p.add_argument(
        "--json-out",
        type=pathlib.Path,
        default=None,
        help="Optional path to write a JSON summary of the action",
    )
    p.add_argument(
        "--print-json",
        action="store_true",
        help="Print a JSON summary to stdout after completion",
    )
    p.add_argument(
        "--list-ports",
        action="store_true",
        help="List available serial ports and exit",
    )

    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument("--target-um", type=float, help="Absolute target height in micrometers")
    group.add_argument("--target-mm", type=float, help="Absolute target height in millimeters")
    group.add_argument(
        "--zero-here",
        action="store_true",
        help="Set the current physical location as 0 steps in the local state file without moving",
    )
    group.add_argument(
        "--status",
        action="store_true",
        help="Print the currently stored position without moving",
    )
    group.add_argument(
        "--set-current-steps",
        type=int,
        help="Force the stored current position to this step count without moving",
    )
    group.add_argument(
        "--set-current-um",
        type=float,
        help="Force the stored current position to this height in um without moving",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()

    if args.list_ports:
        list_serial_ports()
        return 0

    state_path = args.state_file.resolve()

    try:
        state = load_state(state_path)
    except Exception as exc:
        print(f"Error loading state file: {exc}", file=sys.stderr)
        return 1

    current_before = state.current_steps

    try:
        if args.status:
            summary = build_summary(
                action="status",
                state_path=state_path,
                current_steps_before=current_before,
                current_steps_after=state.current_steps,
                target_steps=None,
                port=None,
                baud=args.baud,
            )
            print_human_summary(summary)

        elif args.zero_here:
            state.current_steps = 0
            save_state(state_path, state, note="zero-here")
            summary = build_summary(
                action="zero-here",
                state_path=state_path,
                current_steps_before=current_before,
                current_steps_after=state.current_steps,
                target_steps=0,
                port=None,
                baud=args.baud,
            )
            print_human_summary(summary)

        elif args.set_current_steps is not None:
            state.current_steps = int(args.set_current_steps)
            save_state(state_path, state, note="set-current-steps")
            summary = build_summary(
                action="set-current-steps",
                state_path=state_path,
                current_steps_before=current_before,
                current_steps_after=state.current_steps,
                target_steps=state.current_steps,
                port=None,
                baud=args.baud,
            )
            print_human_summary(summary)

        elif args.set_current_um is not None:
            new_steps = steps_from_target_um(float(args.set_current_um))
            state.current_steps = new_steps
            save_state(state_path, state, note="set-current-um")
            summary = build_summary(
                action="set-current-um",
                state_path=state_path,
                current_steps_before=current_before,
                current_steps_after=state.current_steps,
                target_steps=state.current_steps,
                port=None,
                baud=args.baud,
            )
            print_human_summary(summary)

        else:
            if not args.port:
                print("Error: --port is required for motion commands.", file=sys.stderr)
                return 2

            target_um = float(args.target_um) if args.target_um is not None else float(args.target_mm) * 1000.0
            target_steps = steps_from_target_um(target_um)
            delta_steps = target_steps - state.current_steps

            if delta_steps == 0:
                summary = build_summary(
                    action="move-noop",
                    state_path=state_path,
                    current_steps_before=current_before,
                    current_steps_after=state.current_steps,
                    target_steps=target_steps,
                    port=args.port,
                    baud=args.baud,
                )
                print_human_summary(summary)
            else:
                with open_serial_port(args.port, args.baud, args.settle) as ser:
                    startup_lines = drain_serial(ser, args.startup_read)
                    move_result = send_relative_steps_and_wait(ser, delta_steps, args.timeout)

                expected_device_delta_steps = int(STAGE_DIRECTION_SIGN * delta_steps)
                if move_result.confirmed_delta_steps is not None and move_result.confirmed_delta_steps != expected_device_delta_steps:
                    raise StageProtocolError(
                        f"Arduino reported Moving {move_result.confirmed_delta_steps} steps, expected {expected_device_delta_steps}"
                    )

                state.current_steps = target_steps
                save_state(state_path, state, note=f"move to {target_steps} steps")

                summary = build_summary(
                    action="move-absolute",
                    state_path=state_path,
                    current_steps_before=current_before,
                    current_steps_after=state.current_steps,
                    target_steps=target_steps,
                    port=args.port,
                    baud=args.baud,
                )
                print_human_summary(summary)

                if startup_lines:
                    print("Arduino startup:")
                    for line in startup_lines:
                        print(f"  {line}")
                if move_result.lines:
                    print("Arduino motion log:")
                    for line in move_result.lines:
                        print(f"  {line}")
                    print(f"Motion duration: {move_result.duration_s:.3f} s")

        if args.json_out is not None:
            atomic_write_json(args.json_out.resolve(), summary.to_dict())
        if args.print_json:
            print(json.dumps(summary.to_dict(), indent=2, sort_keys=True))
        return 0

    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
