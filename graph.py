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
    return df


def plot_power_vs_target(datasets, title, fname):
    plt.figure(figsize=PLOT_SIZE)
    for ds in datasets:
        x_col = _pick_column(ds['df'], 'Rate', 'QPS', 'target', context='power vs target (x)')
        y_col = _pick_column(ds['df'], 'Power', 'power', context='power vs target (y)')
        plt.scatter(
            ds['df'][x_col],
            ds['df'][y_col],
            label=ds['label'],
            marker=ds['marker'],
            s=10,
            color=ds['color'],
        )
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
        util_col = _pick_column(ds['df'], 'Util', 'util', context='util vs ops/latency (util)')
        ops_col = _pick_column(ds['df'], 'QPS', 'Ops', 'ops', context='util vs ops/latency (ops)')
        ax.scatter(
            ds['df'][util_col],
            ds['df'][ops_col],
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
        util_col = _pick_column(ds['df'], 'Util', 'util', context='util vs ops/latency (util)')
        lat_col = _pick_column(ds['df'], 'P99_Latency', 'latency', 'Latency', context='util vs ops/latency (latency)')
        ax_right.scatter(
            ds['df'][util_col],
            ds['df'][lat_col],
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
        x_col = _pick_column(ds['df'], 'Util', 'util', context='power vs util (x)')
        y_col = _pick_column(ds['df'], 'Power', 'power', context='power vs util (y)')
        plt.scatter(
            ds['df'][x_col],
            ds['df'][y_col],
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


def main():
    parser = argparse.ArgumentParser(
        description='Plot memcached-mutilate experiment graphs from CSV files. Provide 1 or more CSVs for overlay comparison.'
    )
    parser.add_argument('csv', nargs='+', help='Input CSV file(s) (comma-separated). Provide 1 or more files.')
    parser.add_argument('--labels', nargs='+', default=None, help='Legend label(s) matching the CSV inputs')
    parser.add_argument('--title', default=None, help='Plot title prefix (defaults to CSV stem(s))')
    parser.add_argument('--outdir', default='.', help='Output directory for PNGs')
    parser.add_argument('--prefix', default=None, help='Filename prefix for PNGs (defaults to CSV stem(s))')
    args = parser.parse_args()

    if len(args.csv) < 1:
        raise SystemExit('Please provide at least 1 CSV file.')

    if args.labels is not None and len(args.labels) != len(args.csv):
        raise SystemExit('If provided, --labels must match the number of CSV files.')

    csv_paths = [Path(p) for p in args.csv]
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
        datasets.append(
            {
                'df': read_and_clean_csv(str(csv_path)),
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


if __name__ == '__main__':
    main()