import os
import re
import csv
import sys
import numpy as np
import pandas as pd

def parse_mutilate_file(filepath):
    """
    Parse mutilate master log; if a matching .lat file exists, compute p90/p99 from raw samples.
    """
    base = os.path.splitext(filepath)[0]
    lat_path = base.replace("mutilate_master_qps_", "mutilate_lat_qps_") + ".lat"

    total_qps = None
    avg_lat = p90_lat = p99_lat = None

    # First parse QPS and fallback percentiles from the log
    with open(filepath, "r") as f:
        content = f.read()
    for line in content.strip().splitlines():
        if line.startswith("read"):
            parts = re.split(r'\s+', line)
            try:
                avg_lat = float(parts[1])
                p90_lat = float(parts[6])
                p99_lat = float(parts[8])
            except (IndexError, ValueError):
                continue
        if "Total QPS =" in line:
            try:
                total_qps = float(re.search(r'Total QPS = ([\d.]+)', line).group(1))
            except AttributeError:
                continue

    # If raw latency samples exist, recompute percentiles from them (latency is in usec)
    if os.path.exists(lat_path):
        samples = []
        with open(lat_path, "r") as lf:
            # format: "<op> <latency_us>" (e.g., "GET 123.4")
            for ln in lf:
                ln = ln.strip()
                if not ln:
                    continue
                parts = ln.split()
                if len(parts) < 2:
                    continue
                try:
                    val = float(parts[1])
                    samples.append(val)
                except ValueError:
                    continue
        if samples:
            arr = np.array(samples)
            avg_lat = float(np.mean(arr))
            p90_lat = float(np.percentile(arr, 90))
            p99_lat = float(np.percentile(arr, 99))

    if None in (avg_lat, p90_lat, p99_lat, total_qps):
        raise ValueError(f"Could not parse mutilate data in {filepath}")

    return total_qps, avg_lat, p90_lat, p99_lat

def parse_powerstat_file(filepath):
    with open(filepath, 'r') as file:
        lines = file.readlines()

    start_index = 20  # line 21, 0-based
    end_index = next((i for i, line in enumerate(lines[start_index:], start=start_index) if line.startswith('-')), len(lines))

    data_lines = lines[start_index:end_index]
    data_lines = [line.strip() for line in data_lines if line.strip()]
    data_lines = [line.split() for line in data_lines]
    data_lines = [[line[4], line[12]] for line in data_lines if len(line) > 12]

    df = pd.DataFrame(data_lines, columns=['util', 'power'])
    # print(df)
    df['util'] = pd.to_numeric(df['util'], errors='coerce')
    df['power'] = pd.to_numeric(df['power'], errors='coerce')
    # print(df)
    df = df.dropna()
    # print(df)
    # print(df['util'].mean(), df['power'].mean())
    # df = df[(df['util'] >= 0.9 * df['util'].mean()) & (df['util'] <= 1.1 * df['util'].mean())]
    # print(df)
    df = df[(df['power'] >= 0.9 * df['power'].mean()) & (df['power'] <= 1.1 * df['power'].mean())]
    # print(df)
    average_util = df['util'].mean()
    average_power = df['power'].mean()
    std_power = df['power'].std()
    return average_power, std_power, 100 - average_util

def collect_logs_from_directory(run_dir):
    results = []
    for root, _, files in os.walk(run_dir):
        mutilate_files = [
            f for f in files
            if f.startswith("mutilate_master_qps_") and f.endswith(".log")
        ]
        powerstat_files = [f for f in files if f.startswith("powerstat_rate_")]
        mutilate_rates = []
        powerstat_rates = []
        for i in mutilate_files:
            rate = int(i.split("_")[-1].split(".")[0])
            mutilate_rates.append(rate)
        for i in powerstat_files:
            rate = int(i.split("_")[-1].split(".")[0])
            powerstat_rates.append(rate)
        mutilate_rates = sorted(set(mutilate_rates))
        powerstat_rates = sorted(set(powerstat_rates))

        for rate in mutilate_rates:
            mutilate_file = os.path.join(root, f"mutilate_master_qps_{rate}.log")
            powerstat_file = os.path.join(root, f"powerstat_rate_{rate}.txt")
            try:
                total_qps, avg_lat, p90_lat, p99_lat = parse_mutilate_file(mutilate_file)
                avg_watts, stddev_watts, cpu_util = parse_powerstat_file(powerstat_file)
                results.append([rate, total_qps, avg_lat, p90_lat, p99_lat,
                                avg_watts, stddev_watts, cpu_util])
            except (ValueError, FileNotFoundError) as e:
                print(f"[!] Error processing {mutilate_file} or {powerstat_file}: {e}")
    return results

def write_csv(results, output_file="aggregated_results.csv"):
    header = ["Rate", "QPS", "Avg_Latency", "P90_Latency", "P99_Latency",
              "Power", "StdDev", "Util"]
    with open(output_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(results)

if __name__ == "__main__":
    run_directory = sys.argv[1]
    timestamp = sys.argv[2]
    results = collect_logs_from_directory(run_directory)
    machine_name = run_directory.split("/")[0].split("_")[2]
    results.sort(key=lambda x: int(x[0]))  # Sort by rate
    write_csv(results, output_file=f"aggregated_results_{machine_name}_{timestamp}.csv")
    print(f"[✓] Aggregated CSV written with {len(results)} entries.")

# print(parse_powerstat_file("experiment_logs_c6620fhtoff/run_2025-06-16_19-27-49/powerstat_rate_6200000.txt"))
