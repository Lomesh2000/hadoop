#!/usr/bin/env python3
"""
NASA Log Analytics Reporting & Visualization Engine
Queries the relational database (SQLite/MySQL) and generates:
1. Formatted CLI / ASCII tabular summaries using tabulate
2. CSV analytical summaries
3. High-resolution visual charts:
   - Daily traffic trends (Requests & Volume)
   - Top 10 requested resources bar chart
   - Hourly error rate time-series timeline
"""

import sys
import os
import sqlite3
import argparse
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns
from tabulate import tabulate

DEFAULT_SQLITE_PATH = os.environ.get('SQLITE_PATH', '/tmp/nasalog_analytics.db')

class AnalyticsReporter:
    def __init__(self, db_type: str = "sqlite", **db_kwargs):
        self.db_type = db_type
        self.db_kwargs = db_kwargs
        self.sqlite_path = db_kwargs.get("sqlite_path", DEFAULT_SQLITE_PATH)
        self.conn = None
        self._init_db()

    def _init_db(self):
        if self.db_type == "sqlite":
            self.conn = sqlite3.connect(self.sqlite_path)
        elif self.db_type == "mysql":
            import pymysql
            self.conn = pymysql.connect(
                host=self.db_kwargs.get("host", os.environ.get("DB_HOST", "localhost")),
                port=int(self.db_kwargs.get("port", os.environ.get("DB_PORT", 3306))),
                user=self.db_kwargs.get("user", os.environ.get("DB_USER", "root")),
                password=self.db_kwargs.get("password", os.environ.get("DB_PASSWORD", "")),
                database=self.db_kwargs.get("database", os.environ.get("DB_NAME", "nasadb"))
            )

    def generate_ascii_tables(self) -> str:
        out = []
        out.append("\n" + "=" * 80)
        out.append("                 NASA LOG ANALYTICS EXECUTIVE DASHBOARD                 ")
        out.append("=" * 80 + "\n")

        # Q1: Daily Traffic
        df_q1 = pd.read_sql_query("SELECT log_date AS Date, total_requests AS Requests, total_bytes AS Bytes FROM daily_traffic ORDER BY log_date ASC;", self.conn)
        out.append("--- [Q1] DAILY TRAFFIC BREAKDOWN ---")
        if not df_q1.empty:
            df_q1['MB Transferred'] = (df_q1['Bytes'] / (1024 * 1024)).round(2)
            out.append(tabulate(df_q1, headers='keys', tablefmt='psql', showindex=False))
            out.append(f"Total Period Requests: {df_q1['Requests'].sum():,} | Total Volume: {df_q1['MB Transferred'].sum():,.2f} MB\n")
        else:
            out.append("No data available in daily_traffic table.\n")

        # Q2: Top Resources
        df_q2 = pd.read_sql_query("SELECT rank_order AS Rank, resource AS Resource, request_count AS Requests, total_bytes AS Bytes FROM top_resources ORDER BY rank_order ASC LIMIT 10;", self.conn)
        out.append("--- [Q2] TOP 10 MOST REQUESTED RESOURCES ---")
        if not df_q2.empty:
            df_q2['MB Transferred'] = (df_q2['Bytes'] / (1024 * 1024)).round(2)
            out.append(tabulate(df_q2, headers='keys', tablefmt='psql', showindex=False))
            out.append("")
        else:
            out.append("No data available in top_resources table.\n")

        # Q3: Hourly Errors
        df_q3 = pd.read_sql_query("SELECT log_hour AS Hour, total_requests AS TotalReqs, error_requests AS ErrorReqs, error_rate AS ErrorRatePct FROM hourly_errors ORDER BY error_rate DESC LIMIT 10;", self.conn)
        out.append("--- [Q3] TOP 10 HIGHEST HOURLY ERROR SPIKES ---")
        if not df_q3.empty:
            df_q3['ErrorRatePct'] = df_q3['ErrorRatePct'].round(2).astype(str) + "%"
            out.append(tabulate(df_q3, headers='keys', tablefmt='psql', showindex=False))
            out.append("")
        else:
            out.append("No data available in hourly_errors table.\n")

        return "\n".join(out)

    def generate_charts(self, output_dir: str):
        os.makedirs(output_dir, exist_ok=True)
        sns.set_theme(style="whitegrid")

        # 1. Daily Traffic Trend
        df_q1 = pd.read_sql_query("SELECT log_date, total_requests, total_bytes FROM daily_traffic ORDER BY log_date ASC;", self.conn)
        if not df_q1.empty:
            fig, ax1 = plt.subplots(figsize=(12, 5))
            color = 'tab:blue'
            ax1.set_xlabel('Date', fontweight='bold')
            ax1.set_ylabel('Total Requests', color=color, fontweight='bold')
            ax1.plot(df_q1['log_date'], df_q1['total_requests'], color=color, marker='o', linewidth=2)
            ax1.tick_params(axis='y', labelcolor=color)
            plt.xticks(rotation=45, ha='right')

            ax2 = ax1.twinx()
            color = 'tab:orange'
            ax2.set_ylabel('Total Data Volume (MB)', color=color, fontweight='bold')
            ax2.plot(df_q1['log_date'], df_q1['total_bytes'] / (1024*1024), color=color, marker='s', linestyle='--', linewidth=2)
            ax2.tick_params(axis='y', labelcolor=color)

            plt.title('NASA Web Server Daily Traffic & Data Volume', fontsize=14, fontweight='bold')
            fig.tight_layout()
            chart_q1_path = os.path.join(output_dir, 'daily_traffic_trend.png')
            plt.savefig(chart_q1_path, dpi=200)
            plt.close()

        # 2. Top 10 Resources Bar Chart
        df_q2 = pd.read_sql_query("SELECT resource, request_count FROM top_resources ORDER BY rank_order ASC LIMIT 10;", self.conn)
        if not df_q2.empty:
            plt.figure(figsize=(12, 6))
            ax = sns.barplot(x='request_count', y='resource', data=df_q2, hue='resource', palette='Blues_r', legend=False)
            plt.title('Top 10 Most Frequently Requested NASA Resources', fontsize=14, fontweight='bold')
            plt.xlabel('Total Request Count', fontweight='bold')
            plt.ylabel('Resource URI Path', fontweight='bold')
            plt.tight_layout()
            chart_q2_path = os.path.join(output_dir, 'top_resources.png')
            plt.savefig(chart_q2_path, dpi=200)
            plt.close()

        # 3. Hourly Error Rate Timeline (Time-series)
        df_q3 = pd.read_sql_query("SELECT log_hour, error_rate FROM hourly_errors ORDER BY log_hour ASC;", self.conn)
        if not df_q3.empty:
            plt.figure(figsize=(14, 5))
            plt.plot(range(len(df_q3)), df_q3['error_rate'], color='crimson', linewidth=1.5)
            # Show a subset of ticks for readability
            tick_step = max(1, len(df_q3) // 14)
            tick_locs = list(range(0, len(df_q3), tick_step))
            tick_labels = [df_q3['log_hour'].iloc[i][:13] for i in tick_locs]
            plt.xticks(tick_locs, tick_labels, rotation=45, ha='right', fontsize=9)
            plt.title('NASA Web Server Hourly Error Rate Timeline (%)', fontsize=14, fontweight='bold')
            plt.xlabel('Log Hour', fontweight='bold')
            plt.ylabel('Error Rate (%)', fontweight='bold')
            plt.tight_layout()
            chart_q3_path = os.path.join(output_dir, 'hourly_error_timeline.png')
            plt.savefig(chart_q3_path, dpi=200)
            plt.close()

    def export_csv_summaries(self, output_dir: str):
        os.makedirs(output_dir, exist_ok=True)
        for tbl in ['daily_traffic', 'top_resources', 'hourly_errors', 'data_quality_results']:
            df = pd.read_sql_query(f"SELECT * FROM {tbl};", self.conn)
            df.to_csv(os.path.join(output_dir, f"{tbl}.csv"), index=False)

    def close(self):
        if self.conn:
            self.conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NASA Analytics Reporter")
    parser.add_argument('--output-dir', default='/working_dir/c_d4adba4239bd4987/NOSQL-main/EndTerm_Project/reports', help="Output directory for charts and CSVs")
    parser.add_argument('--db-type', default='sqlite', choices=['sqlite', 'mysql'])
    parser.add_argument('--sqlite-path', default=DEFAULT_SQLITE_PATH)

    args = parser.parse_args()
    reporter = AnalyticsReporter(db_type=args.db_type, sqlite_path=args.sqlite_path)
    ascii_report = reporter.generate_ascii_tables()
    print(ascii_report)
    reporter.generate_charts(os.path.join(args.output_dir, 'charts'))
    reporter.export_csv_summaries(args.output_dir)
    reporter.close()
    print(f"Visual charts and CSV reports generated in {args.output_dir}")
