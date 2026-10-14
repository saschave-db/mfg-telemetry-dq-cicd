"""Physics-lite degradation simulator, used by the telemetry generator.

Each station has a hidden health h in [0, 1]. A degradation episode drives d = 1 - h from 0 to 1
over `duration` seconds. Each failure mode leaves a distinct sensor signature. At d = 1 the
station fails, stops for DOWN_S seconds, and is repaired. Benign drifts and tool-change spikes
add realistic false-alarm patterns. All data is synthetic.
"""
from __future__ import annotations

import numpy as np

from telemetry.config import (
    FAILURE_MODES_BY_TYPE, LINES, NOISE, NOMINAL, PLANTS, SENSORS, STATION_TYPES_BY_POSITION,
)

DOWN_S = 60
MEAN_GAP_S = 90 * 60          # mean time between degradation onsets per station
DURATION_RANGE_S = (240, 600)  # onset to failure
BENIGN_MEAN_GAP_S = 120 * 60
BENIGN_DURATION_RANGE_S = (180, 360)
SPIKE_PROB = 0.003


def build_station_master(seed: int = 7) -> list[dict]:
    """96 stations: 3 plants x 4 lines x 8 positions."""
    rng = np.random.default_rng(seed)
    rows = []
    for plant_id, plant_name in PLANTS.items():
        for line in LINES:
            for pos, stype in enumerate(STATION_TYPES_BY_POSITION, start=1):
                sid = f"{plant_id}-{line}{pos:02d}"
                rows.append(dict(
                    station_id=sid,
                    plant_id=plant_id,
                    plant_name=plant_name,
                    line_id=f"{plant_id}-{line}",
                    position=pos,
                    station_type=stype,
                    criticality=str(rng.choice(["high", "medium", "low"], p=[0.3, 0.5, 0.2])),
                    install_year=int(rng.integers(2012, 2025)),
                    gateway_id=f"gw-{plant_id.lower()}",
                    **{f"nom_{s}": float(NOMINAL[stype][s]) for s in SENSORS},
                ))
    return rows


class Fleet:
    """Vectorized state for a set of stations. Call step(t) once per second."""

    def __init__(self, stations: list[dict], seed: int, t0: float,
                 auto_preventive_prob: float = 0.0, natural_onsets: bool = True):
        self.rng = np.random.default_rng(seed)
        self.stations = stations
        self.n = len(stations)
        self.ids = np.array([s["station_id"] for s in stations])
        self.types = np.array([s["station_type"] for s in stations])
        self.nom = {s: np.array([NOMINAL[t][s] for t in self.types], dtype=float) for s in SENSORS}
        self.offset = {s: self.rng.normal(1.0, 0.03, self.n) for s in SENSORS}
        self.load_phase = self.rng.uniform(0, 2 * np.pi, self.n)
        self.product_step = np.ones(self.n)
        self.auto_preventive_prob = auto_preventive_prob
        self.natural_onsets = natural_onsets

        # Degradation state.
        self.mode = np.array([""] * self.n, dtype=object)
        self.onset = np.full(self.n, np.nan)
        self.duration = np.full(self.n, np.nan)
        self.down_until = np.full(self.n, -np.inf)
        self.prevent_at_d = np.full(self.n, np.inf)
        self.next_onset = t0 + self.rng.exponential(MEAN_GAP_S, self.n)
        # Benign drift state.
        self.b_onset = np.full(self.n, np.nan)
        self.b_duration = np.full(self.n, np.nan)
        self.b_next = t0 + self.rng.exponential(BENIGN_MEAN_GAP_S, self.n)
        self.spike_until = np.full(self.n, -np.inf)

    # ---- control -------------------------------------------------------------------------
    def start_episode(self, i: int, t: float, mode: str | None = None, duration: float | None = None):
        if self.mode[i] or t < self.down_until[i]:
            return None
        modes = FAILURE_MODES_BY_TYPE[self.types[i]]
        self.mode[i] = mode if mode in modes else str(self.rng.choice(modes))
        self.onset[i] = t
        self.duration[i] = duration or self.rng.uniform(*DURATION_RANGE_S)
        self.prevent_at_d[i] = 0.75 if self.rng.random() < self.auto_preventive_prob else np.inf
        return dict(station_id=str(self.ids[i]), event_type="degradation_onset", failure_mode=self.mode[i])

    def repair(self, i: int, t: float, event_type: str = "preventive_repair"):
        mode = self.mode[i]
        self.mode[i] = ""
        self.onset[i] = np.nan
        self.duration[i] = np.nan
        self.down_until[i] = -np.inf
        self.next_onset[i] = t + self.rng.exponential(MEAN_GAP_S)
        return dict(station_id=str(self.ids[i]), event_type=event_type, failure_mode=mode or None)

    def index_of(self, station_id: str) -> int | None:
        hits = np.where(self.ids == station_id)[0]
        return int(hits[0]) if len(hits) else None

    # ---- simulation ----------------------------------------------------------------------
    def degradation(self, t: float) -> np.ndarray:
        d = (t - self.onset) / self.duration
        return np.where(np.isnan(d), 0.0, np.clip(d, 0, 1)) ** 1.6

    def ttf_seconds(self, t: float) -> np.ndarray:
        """Ground-truth seconds to (projected) failure. 0 while down, inf when healthy."""
        ttf = np.where(np.isnan(self.onset), np.inf, self.onset + self.duration - t)
        return np.where(t < self.down_until, 0.0, ttf)

    def step(self, t: float):
        rng, n = self.rng, self.n
        events = []

        # Natural onsets and benign drifts.
        if self.natural_onsets:
            for i in np.where((t >= self.next_onset) & (self.mode == "") & (t >= self.down_until))[0]:
                ev = self.start_episode(int(i), t)
                if ev:
                    events.append(ev)
        for i in np.where((t >= self.b_next) & np.isnan(self.b_onset))[0]:
            self.b_onset[i] = t
            self.b_duration[i] = rng.uniform(*BENIGN_DURATION_RANGE_S)
        b_frac = (t - self.b_onset) / self.b_duration
        done_b = b_frac >= 1
        self.b_next = np.where(done_b, t + rng.exponential(BENIGN_MEAN_GAP_S, n), self.b_next)
        self.b_onset = np.where(done_b, np.nan, self.b_onset)
        db = np.where(np.isnan(b_frac) | done_b, 0.0, 0.35 * (1 - np.abs(2 * np.nan_to_num(b_frac) - 1)))

        d = self.degradation(t)

        # Preventive repair (history only) and failures.
        for i in np.where((self.mode != "") & (d >= self.prevent_at_d))[0]:
            events.append(self.repair(int(i), t, "preventive_repair"))
        d = self.degradation(t)
        for i in np.where((self.mode != "") & (d >= 1.0) & (t >= self.down_until))[0]:
            if self.down_until[i] < t - DOWN_S:  # failure moment
                events.append(dict(station_id=str(self.ids[i]), event_type="failure", failure_mode=self.mode[i]))
                self.down_until[i] = t + DOWN_S
        for i in np.where((self.mode != "") & np.isfinite(self.down_until) & (t >= self.down_until))[0]:
            events.append(self.repair(int(i), t, "corrective_repair"))
        d = self.degradation(t)
        down = t < self.down_until

        # Operating point: load cycle + occasional product changeovers.
        change = rng.random(n) < 1 / 1800
        self.product_step = np.where(change, rng.uniform(0.96, 1.04, n), self.product_step)
        load = (1 + 0.06 * np.sin(2 * np.pi * t / 1800 + self.load_phase)) * self.product_step
        self.spike_until = np.where(rng.random(n) < SPIKE_PROB, t + 2, self.spike_until)
        spike = t < self.spike_until

        r = {}
        for s in SENSORS:
            r[s] = self.nom[s] * self.offset[s] * (1 + NOISE[s] * rng.standard_normal(n))
        r["motor_current_a"] *= load
        r["bearing_temp_c"] *= 1 + 0.15 * (load - 1)
        r["cycle_time_s"] *= 2 - load

        m = self.mode
        bw, oh, sl = (m == "bearing_wear"), (m == "overheating"), (m == "seal_leak")
        r["vibration_rms"] *= 1 + np.where(bw, 3.0 * d ** 2, 0) + np.where(oh, 0.3 * d, 0)
        r["acoustic_db"] += np.where(bw, 10 * d ** 1.5, 0)
        r["bearing_temp_c"] += np.where(bw, 8 * d, 0) + np.where(oh, 30 * d ** 1.3, 0)
        r["motor_current_a"] *= 1 + np.where(oh, 0.25 * d, 0) + np.where(sl, 0.1 * d, 0)
        r["hydraulic_pressure_bar"] *= 1 - np.where(sl, 0.35 * d ** 1.2, 0)
        r["cycle_time_s"] *= 1 + np.where(sl, 0.3 * d, 0)

        # Benign drift: looks like early degradation, then recovers.
        r["bearing_temp_c"] += 12 * db
        r["vibration_rms"] *= 1 + 1.2 * db
        r["motor_current_a"] *= 1 + 0.12 * db

        # Tool-change spikes.
        r["vibration_rms"] = np.where(spike, r["vibration_rms"] * rng.uniform(2, 3.5, n), r["vibration_rms"])
        r["acoustic_db"] = np.where(spike, r["acoustic_db"] + 6, r["acoustic_db"])

        # Down state.
        r["spindle_rpm"] = np.where(down, 0.0, r["spindle_rpm"])
        r["motor_current_a"] = np.where(down, 0.05 * self.nom["motor_current_a"], r["motor_current_a"])
        r["vibration_rms"] = np.where(down, 0.05, r["vibration_rms"])
        r["acoustic_db"] = np.where(down, 40.0, r["acoustic_db"])
        r["cycle_time_s"] = np.where(down, 0.0, r["cycle_time_s"])
        r["bearing_temp_c"] = np.where(down, 0.8 * self.nom["bearing_temp_c"], r["bearing_temp_c"])

        for s in SENSORS:
            r[s] = np.round(np.maximum(r[s], 0.0), 3)
        return r, events
