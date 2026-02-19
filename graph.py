import pandas as pd
import matplotlib.pyplot as plt
import argparse
from pathlib import Path

PLOT_SIZE = (10, 6)

# Color palette for multiple datasets
COLORS = [
    '#1f77b4',  # Blue
    '#ff7f0e',  # Orange
    '#2ca02c',  # Green
    '#d62728',  # Red
    '#9467bd',  # Purple
    '#8c564b',  # Brown
    '#e377c2',  # Pink
    '#7f7f7f',  # Gray
    '#bcbd22',  # Olive
    '#17becf',  # Cyan
]

# Markers for multiple datasets
MARKERS = ['o', 's', '^', 'v', 'd', 'p', '*', 'h', '+', 'x']
LAT_MARKERS = ['x', '^', 'v', 's', 'd', 'p', '*', 'h', '+', 'o']


def _pick_column(df, *candidates, context=''):
    for name in candidates:
        if name in df.columns:
            return name
    raise ValueError(
        f"Missing required column for {context}. Tried {list(candidates)}; found columns: {list(df.columns)}"
    )


def read_and_clean_csv(filename):
    df = pd.read_csv(filename, sep=',', comment='#')
    df = df.replace(',', '', regex=True).apply(pd.to_numeric, errors='coerce')
    # ensure sorted by QPS/Rate for proper line plotting
    for col in ("Rate", "QPS", "target"):
        if col in df.columns:
            df = df.sort_values(by=col)
            break
    return df


def _filter_by_latency(df, cap_ms):
    if cap_ms is None:
        return df
    # Prefer P99_Latency if present, else Avg_Latency
    lat_col = None
    for c in ("P99_Latency", "Avg_Latency", "latency", "Latency"):
        if c in df.columns:
            lat_col = c
            break
    if lat_col is None:
        return df
    cap_us = cap_ms * 1000.0
    return df[df[lat_col] <= cap_us]


def plot_power_vs_target(datasets, title, fname, latency_cap=None, linear_ref=False):
    plt.figure(figsize=PLOT_SIZE)
    ref_done = False
    ref_points = []
    for ds in datasets:
        df = _filter_by_latency(ds['df'], latency_cap)
        if df.empty:
            continue
        df = df.sort_values(by=_pick_column(df, 'Rate', 'QPS', 'target', context='power vs target (x)'))
        x_col = _pick_column(df, 'Rate', 'QPS', 'target', context='power vs target (x)')
        y_col = _pick_column(df, 'Power', 'power', context='power vs target (y)')
        plt.scatter(
            df[x_col],
            df[y_col],
            label=ds['label'],
            marker=ds['marker'],
            s=10,
            color=ds['color'],
        )
        if linear_ref and not ref_done and not df.empty:
            df_sorted = df.sort_values(by=x_col)
            x_max = df_sorted.iloc[-1][x_col]
            y_max = df_sorted.iloc[-1][y_col]
            x_ref = [0, x_max]
            y_ref = [0, y_max]
            plt.plot(x_ref, y_ref, linestyle='--', color='grey', label='Linear ref')
            ref_done = True
            ref_points.append((x_ref, y_ref))
    plt.xlabel('Target Rate')
    plt.ylabel('Power (W)')
    plt.title(title)
    plt.legend()
    plt.grid()
    plt.tight_layout()
    plt.savefig(fname, dpi=1000)
    plt.close()


def plot_util_vs_ops_latency(datasets, title, fname):
    plt.figure(figsize=PLOT_SIZE)

    ax = plt.gca()
    for ds in datasets:
        df = ds['df']
        qps_col = _pick_column(df, 'Rate', 'QPS', 'target', context='util vs ops/latency (qps)')
        util_col = _pick_column(df, 'Util', 'util', context='util vs ops/latency (util)')
        ops_col = _pick_column(df, 'QPS', 'Ops', 'ops', context='util vs ops/latency (ops)')
        df = df.sort_values(by=qps_col)
        ax.scatter(
            df[util_col],
            df[ops_col],
            label=f"{ds['label']} Ops/sec",
            marker=ds['marker'],
            s=10,
            color=ds['color'],
        )
    ax.set_xlabel('Utilization (%)')
    ax.set_ylabel('Ops/sec')
    ax.set_title(title)
    ax.grid()

    ax_right = ax.twinx()
    for ds in datasets:
        df = ds['df']
        qps_col = _pick_column(df, 'Rate', 'QPS', 'target', context='util vs ops/latency (qps)')
        util_col = _pick_column(df, 'Util', 'util', context='util vs ops/latency (util)')
        lat_col = _pick_column(df, 'P99_Latency', 'latency', 'Latency', context='util vs ops/latency (latency)')
        df = df.sort_values(by=qps_col)
        lat_ms = df[lat_col] / 1000.0  # mutilate reports µs
        ax_right.scatter(
            df[util_col],
            lat_ms,
            label=f"{ds['label']} P99 Latency",
            marker=ds['lat_marker'],
            s=7,
            color=ds['color'],
        )
    ax_right.set_ylabel('Latency (ms)')

    h1, l1 = ax.get_legend_handles_labels()
    h2, l2 = ax_right.get_legend_handles_labels()
    ax.legend(h1 + h2, l1 + l2, loc='upper left')

    plt.tight_layout()
    plt.savefig(fname, dpi=1000)
    plt.close()


def plot_power_vs_utilization(datasets, title, fname):
    plt.figure(figsize=PLOT_SIZE)
    for ds in datasets:
        df = ds['df']
        x_col = _pick_column(df, 'Util', 'util', context='power vs util (x)')
        y_col = _pick_column(df, 'Power', 'power', context='power vs util (y)')
        plt.scatter(
            df[x_col],
            df[y_col],
            label=ds['label'],
            marker=ds['marker'],
            s=10,
            color=ds['color'],
        )
    plt.xlabel('Utilization (%)')
    plt.ylabel('Power (W)')
    plt.title(title)
    plt.legend()
    plt.grid()
    plt.tight_layout()
    plt.savefig(fname, dpi=1000)
    plt.close()


def plot_qps_vs_cpu(datasets, title, fname):
    plt.figure(figsize=PLOT_SIZE)
    for ds in datasets:
        df = ds['df']
        qps_col = _pick_column(df, 'Rate', 'QPS', 'target', context='qps vs cpu (x)')
        cpu_col = _pick_column(df, 'Util', 'util', context='qps vs cpu (y)')
        plt.scatter(
            df[qps_col],
            df[cpu_col],
            label=ds['label'],
            marker=ds['marker'],
            s=10,
            color=ds['color'],
        )
    plt.xlabel('QPS')
    plt.ylabel('CPU Utilization (%)')
    plt.title(title)
    plt.legend()
    plt.grid()
    plt.tight_layout()
    plt.savefig(fname, dpi=1000)
    plt.close()


def plot_all_latency_vs_qps(datasets, title, fname, latency_cap=None):
    plt.figure(figsize=PLOT_SIZE)
    for ds in datasets:
        df = ds['df']
        qps_col = _pick_column(df, 'Rate', 'QPS', 'target', context='latency vs qps (x)')
        # We expect P50, P90, P95, P99 columns (in µs). If missing, skip.
        lat_cols = [c for c in ['P50_Latency', 'P90_Latency', 'P95_Latency', 'P99_Latency'] if c in df.columns]
        for c in lat_cols:
            df_plot = df
            if latency_cap is not None and c == 'P99_Latency':
                df_plot = df[df[c] <= latency_cap * 1000]
            plt.plot(
                df_plot[qps_col],
                df_plot[c] / 1000.0,  # µs -> ms
                label=f"{ds['label']} {c.replace('_Latency','')}",
                linestyle='--',
                marker=None,
                color=ds['color'],
                alpha=0.8,
            )
    plt.xlabel('QPS')
    plt.ylabel('Latency (ms)')
    plt.title(title)
    plt.legend()
    plt.grid()
    plt.tight_layout()
    plt.savefig(fname, dpi=1000)
    plt.close()


def plot_target_vs_actual_qps(datasets, title, fname):
    plt.figure(figsize=PLOT_SIZE)
    for ds in datasets:
        df = ds['df']
        target_col = _pick_column(df, 'Rate', 'QPS', 'target', context='target vs actual (target)')
        actual_col = _pick_column(df, 'QPS', 'Ops', 'ops', context='target vs actual (actual)')
        plt.scatter(
            df[target_col],
            df[actual_col],
            label=ds['label'],
            marker=ds['marker'],
            s=10,
            color=ds['color'],
        )
    # reference y=x line
    all_targets = pd.concat([ds['df'][_pick_column(ds['df'], 'Rate', 'QPS', 'target', context='agg')] for ds in datasets])
    if not all_targets.empty:
        t_min, t_max = all_targets.min(), all_targets.max()
        plt.plot([t_min, t_max], [t_min, t_max], linestyle='--', color='grey', label='Ideal y=x')
    plt.xlabel('Target QPS')
    plt.ylabel('Actual QPS')
    plt.title(title)
    plt.legend()
    plt.grid()
    plt.tight_layout()
    plt.savefig(fname, dpi=1000)
    plt.close()


def plot_power_and_latency_vs_qps(datasets, title, fname, latency_cap=None):
    plt.figure(figsize=PLOT_SIZE)
    ax_left = plt.gca()
    ax_right = ax_left.twinx()
    ref_plotted = False
    ref_points = []

    for ds in datasets:
        df = ds['df']
        qps_col = _pick_column(df, 'Rate', 'QPS', 'target', context='power/latency vs qps (x)')
        power_col = _pick_column(df, 'Power', 'power', context='power/latency vs qps (power)')
        lat_col = _pick_column(df, 'P99_Latency', 'Latency', 'latency', 'p99', context='power/latency vs qps (latency)')

        if latency_cap is not None:
            df = df[df[lat_col] <= latency_cap * 1000]  # cap provided in ms, data in µs
        lat_ms = df[lat_col] / 1000.0  # convert µs -> ms
        df = df.sort_values(by=qps_col)
        ax_left.plot(df[qps_col], df[power_col], marker=ds['marker'], color=ds['color'],
                     label=f"{ds['label']} Power", linestyle='-')
        ax_right.plot(df[qps_col], lat_ms, marker=ds['lat_marker'], color=ds['color'],
                      label=f"{ds['label']} P99 Latency", linestyle='--')
        if not ref_plotted and not df.empty:
            df_sorted = df.sort_values(by=qps_col)
            x_max = df_sorted.iloc[-1][qps_col]
            y_max = df_sorted.iloc[-1][power_col]
            ref_points.append(([0, x_max], [0, y_max]))
            ref_plotted = True

    ax_left.set_xlabel('QPS')
    ax_left.set_ylabel('Power (W)')
    ax_right.set_ylabel('Avg Latency (ms)')
    ax_left.set_title(title)

    # linear reference
    if ref_points:
        x_ref, y_ref = ref_points[0]
        ax_left.plot(x_ref, y_ref, linestyle='--', color='grey', label='Linear ref')

    # combine legends
    h1, l1 = ax_left.get_legend_handles_labels()
    h2, l2 = ax_right.get_legend_handles_labels()
    ax_left.legend(h1 + h2, l1 + l2, loc='upper left')
    ax_left.grid(True)

    plt.tight_layout()
    plt.savefig(fname, dpi=1000)
    plt.close()


def main():
    parser = argparse.ArgumentParser(
        description='Plot memcached/redis experiment graphs from CSV files. Provide 1 or more CSVs for overlay comparison.'
    )
    parser.add_argument('csv', nargs='+', help='Input CSV file(s) located under Aggregated_Results (filenames only or paths).')
    parser.add_argument('--labels', nargs='+', default=None, help='Legend label(s) matching the CSV inputs')
    parser.add_argument('--title', default=None, help='Plot title prefix (defaults to CSV stem(s))')
    parser.add_argument('--outdir', default='.', help='Output directory for PNGs')
    parser.add_argument('--prefix', default=None, help='Filename prefix for PNGs (defaults to CSV stem(s))')
    parser.add_argument('--latency-cap', type=float, default=None,
                        help='Maximum P99 latency (ms); points above are dropped from all plots.')
    args = parser.parse_args()

    if len(args.csv) < 1:
        raise SystemExit('Please provide at least 1 CSV file.')

    if args.labels is not None and len(args.labels) != len(args.csv):
        raise SystemExit('If provided, --labels must match the number of CSV files.')

    csv_paths = [Path("Aggregated_Results") / Path(p) for p in args.csv]
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    stems = [p.stem for p in csv_paths]
    
    # Generate default title and prefix based on number of files
    if len(stems) == 1:
        default_title_prefix = stems[0]
        default_prefix = stems[0]
    else:
        default_title_prefix = ' vs '.join(stems)
        default_prefix = '_vs_'.join(stems)
    
    title_prefix = args.title or default_title_prefix
    prefix = args.prefix or default_prefix

    labels = args.labels or stems
    
    # Get colors and markers for all datasets
    num_datasets = len(csv_paths)
    colors = [COLORS[i % len(COLORS)] for i in range(num_datasets)]
    markers = [MARKERS[i % len(MARKERS)] for i in range(num_datasets)]
    lat_markers = [LAT_MARKERS[i % len(LAT_MARKERS)] for i in range(num_datasets)]

    datasets = []
    for i, csv_path in enumerate(csv_paths):
        df_raw = read_and_clean_csv(str(csv_path))
        df_filtered = _filter_by_latency(df_raw, args.latency_cap)
        datasets.append(
            {
                'df': df_filtered,
                'label': labels[i],
                'color': colors[i],
                'marker': markers[i],
                'lat_marker': lat_markers[i],
            }
        )

    plot_power_vs_target(
        datasets,
        title=f'{title_prefix}: Power vs Target Rate',
        fname=str(outdir / f'{prefix}-power-vs-target.png'),
        latency_cap=args.latency_cap,
        linear_ref=True,
    )
    plot_util_vs_ops_latency(
        datasets,
        title=f'{title_prefix}: Util vs Ops & Latency',
        fname=str(outdir / f'{prefix}-util-vs-ops-latency.png'),
    )
    plot_power_vs_utilization(
        datasets,
        title=f'{title_prefix}: Power vs Utilization',
        fname=str(outdir / f'{prefix}-power-vs-util.png'),
    )
    plot_power_and_latency_vs_qps(
        datasets,
        title=f'{title_prefix}: Power & Avg Latency vs QPS',
        fname=str(outdir / f'{prefix}-power-latency-vs-qps.png'),
        latency_cap=args.latency_cap,
    )
    plot_qps_vs_cpu(
        datasets,
        title=f'{title_prefix}: QPS vs CPU Utilization',
        fname=str(outdir / f'{prefix}-qps-vs-cpu.png'),
    )
    plot_all_latency_vs_qps(
        datasets,
        title=f'{title_prefix}: Latencies vs QPS',
        fname=str(outdir / f'{prefix}-latencies-vs-qps.png'),
        latency_cap=args.latency_cap,
    )
    plot_target_vs_actual_qps(
        datasets,
        title=f'{title_prefix}: Target vs Actual QPS',
        fname=str(outdir / f'{prefix}-target-vs-actual-qps.png'),
    )


if __name__ == '__main__':
    main()
