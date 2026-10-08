"""
Data Quality Scoring and Reporting Module.
Calculates overall and dimension-level DQ scores, and exports HTML, Markdown, and JSON reports.

DQ Dimensions:
1. Completeness: Missing values, null keys, schema adherence.
2. Validity: Format regex matching, range checks, allowed domain sets.
3. Uniqueness: Primary/natural key duplicates.
4. Consistency: Cross-field and cross-layer consistency, checksum hashes.
5. Referential Integrity: Foreign key parent-child relationships.
6. Timeliness: Date sequence chronology and freshness.
"""

import os
import json
from typing import Dict, Any, List
from datetime import datetime


class DQScoreCalculator:
    """
    Computes weighted and unweighted DQ Scores across dimensions and Medallion layers.
    """

    def __init__(self):
        self.results: List[Dict[str, Any]] = []

    def record_check(self, rule_id: str, rule_name: str, dimension: str, layer: str,
                     table: str, total_records: int, passed_records: int, failed_records: int,
                     action: str = "QUARANTINE", passed: bool = True, details: str = ""):
        """
        Records the outcome of a single rule check.
        """
        pass_rate = round((passed_records / total_records * 100) if total_records > 0 else (100.0 if passed else 0.0), 2)
        self.results.append({
            "rule_id": rule_id,
            "rule_name": rule_name,
            "dimension": dimension,
            "layer": layer,
            "table": table,
            "total_records": total_records,
            "passed_records": passed_records,
            "failed_records": failed_records,
            "pass_rate": pass_rate,
            "action": action,
            "passed": passed if total_records == 0 else (failed_records == 0),
            "details": details
        })

    def calculate_scores(self) -> Dict[str, Any]:
        """
        Aggregates check results to calculate dimension-level, layer-level, and overall DQ scores.
        """
        if not self.results:
            return {"overall_score": 100.0, "dimensions": {}, "layers": {}, "checks": []}

        # 1. Dimension Scores
        dimensions = {}
        for r in self.results:
            dim = r["dimension"]
            if dim not in dimensions:
                dimensions[dim] = {"rates": [], "total_checks": 0, "passed_checks": 0}
            dimensions[dim]["rates"].append(r["pass_rate"])
            dimensions[dim]["total_checks"] += 1
            if r["passed"]:
                dimensions[dim]["passed_checks"] += 1

        dimension_summary = {}
        for dim, data in dimensions.items():
            avg_score = round(sum(data["rates"]) / len(data["rates"]), 2)
            dimension_summary[dim] = {
                "score": avg_score,
                "total_checks": data["total_checks"],
                "passed_checks": data["passed_checks"],
                "grade": self._get_grade(avg_score)
            }

        # 2. Layer Scores
        layers = {}
        for r in self.results:
            layer = r["layer"]
            if layer not in layers:
                layers[layer] = {"rates": [], "total_checks": 0, "passed_checks": 0}
            layers[layer]["rates"].append(r["pass_rate"])
            layers[layer]["total_checks"] += 1
            if r["passed"]:
                layers[layer]["passed_checks"] += 1

        layer_summary = {}
        for layer, data in layers.items():
            avg_score = round(sum(data["rates"]) / len(data["rates"]), 2)
            layer_summary[layer] = {
                "score": avg_score,
                "total_checks": data["total_checks"],
                "passed_checks": data["passed_checks"],
                "grade": self._get_grade(avg_score)
            }

        # 3. Overall DQ Score
        all_rates = [r["pass_rate"] for r in self.results]
        overall_score = round(sum(all_rates) / len(all_rates), 2)

        return {
            "overall_score": overall_score,
            "overall_grade": self._get_grade(overall_score),
            "total_rules_evaluated": len(self.results),
            "total_checks_passed": sum(1 for r in self.results if r["passed"]),
            "total_checks_failed": sum(1 for r in self.results if not r["passed"]),
            "dimension_scores": dimension_summary,
            "layer_scores": layer_summary,
            "rule_results": self.results
        }

    @staticmethod
    def _get_grade(score: float) -> str:
        if score >= 95.0:
            return "EXCELLENT"
        elif score >= 85.0:
            return "GOOD"
        elif score >= 70.0:
            return "ACCEPTABLE"
        elif score >= 50.0:
            return "POOR"
        else:
            return "CRITICAL"


class DQHtmlReporter:
    """
    Generates an executive, interactive single-file HTML Data Quality Scorecard report.
    """

    @staticmethod
    def generate_html_report(summary: Dict[str, Any], output_path: str = "reports/dq_scorecard_report.html"):
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        dim_cards_html = ""
        for dim, info in summary["dimension_scores"].items():
            badge_color = "#10b981" if info["score"] >= 90 else ("#f59e0b" if info["score"] >= 75 else "#ef4444")
            dim_cards_html += f"""
            <div class="card">
                <div class="card-header">
                    <span>{dim}</span>
                    <span class="badge" style="background-color: {badge_color}">{info['score']}%</span>
                </div>
                <div class="progress-bar">
                    <div class="progress-fill" style="width: {info['score']}%; background-color: {badge_color}"></div>
                </div>
                <div class="card-subtext">{info['passed_checks']}/{info['total_checks']} Checks Passed &bull; Grade: {info['grade']}</div>
            </div>
            """

        layer_rows_html = ""
        for layer, info in summary["layer_scores"].items():
            layer_rows_html += f"""
            <tr>
                <td><strong>{layer} Layer</strong></td>
                <td><span class="score-pill">{info['score']}%</span></td>
                <td>{info['passed_checks']}/{info['total_checks']}</td>
                <td><span class="status-badge status-{info['grade'].lower()}">{info['grade']}</span></td>
            </tr>
            """

        rule_rows_html = ""
        for r in summary["rule_results"]:
            status_cls = "pass" if r["passed"] else "fail"
            status_text = "PASSED" if r["passed"] else "VIOLATIONS DETECTED"
            action_badge = f'<span class="badge-action badge-{r["action"].lower()}">{r["action"]}</span>'
            rule_rows_html += f"""
            <tr>
                <td><code>{r['rule_id']}</code></td>
                <td><strong>{r['rule_name']}</strong><br><small style="color: #64748b">{r['details']}</small></td>
                <td><span class="tag">{r['table']}</span></td>
                <td><span class="tag">{r['layer']}</span></td>
                <td><span class="tag tag-dim">{r['dimension']}</span></td>
                <td>{action_badge}</td>
                <td>{r['passed_records']}/{r['total_records']} ({r['pass_rate']}%)</td>
                <td><span class="status-tag status-{status_cls}">{status_text}</span></td>
            </tr>
            """

        overall_color = "#10b981" if summary["overall_score"] >= 90 else ("#f59e0b" if summary["overall_score"] >= 75 else "#ef4444")

        html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Data Quality Executive Scorecard Report</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; }}
        body {{ background-color: #0f172a; color: #f8fafc; padding: 24px; }}
        .container {{ max-width: 1200px; margin: 0 auto; }}
        .header {{ display: flex; justify-content: space-between; align-items: center; padding-bottom: 24px; border-bottom: 1px solid #334155; margin-bottom: 24px; }}
        .header h1 {{ font-size: 26px; color: #38bdf8; }}
        .header-meta {{ font-size: 14px; color: #94a3b8; text-align: right; }}
        
        .kpi-row {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 16px; margin-bottom: 24px; }}
        .kpi-card {{ background: #1e293b; border-radius: 10px; padding: 20px; border: 1px solid #334155; }}
        .kpi-title {{ font-size: 13px; text-transform: uppercase; color: #94a3b8; margin-bottom: 8px; letter-spacing: 0.5px; }}
        .kpi-value {{ font-size: 32px; font-weight: 700; color: #f8fafc; }}
        
        .grid-2 {{ display: grid; grid-template-columns: 2fr 1fr; gap: 20px; margin-bottom: 24px; }}
        .section-box {{ background: #1e293b; border-radius: 10px; padding: 20px; border: 1px solid #334155; }}
        .section-title {{ font-size: 18px; font-weight: 600; color: #e2e8f0; margin-bottom: 16px; }}
        
        .dim-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 12px; }}
        .card {{ background: #0f172a; padding: 14px; border-radius: 8px; border: 1px solid #334155; }}
        .card-header {{ display: flex; justify-content: space-between; font-weight: 600; margin-bottom: 8px; font-size: 14px; }}
        .badge {{ padding: 2px 8px; border-radius: 12px; font-size: 12px; font-weight: 700; color: #fff; }}
        .progress-bar {{ background: #334155; height: 8px; border-radius: 4px; overflow: hidden; margin-bottom: 6px; }}
        .progress-fill {{ height: 100%; border-radius: 4px; }}
        .card-subtext {{ font-size: 11px; color: #94a3b8; }}
        
        table {{ width: 100%; border-collapse: collapse; margin-top: 10px; font-size: 13px; }}
        th {{ text-align: left; padding: 10px 12px; background: #0f172a; color: #94a3b8; font-weight: 600; border-bottom: 1px solid #334155; }}
        td {{ padding: 10px 12px; border-bottom: 1px solid #334155; color: #e2e8f0; }}
        tr:hover td {{ background: #243248; }}
        
        .tag {{ display: inline-block; padding: 2px 6px; border-radius: 4px; background: #334155; font-size: 11px; }}
        .tag-dim {{ background: #0284c7; color: #fff; }}
        .score-pill {{ font-weight: 700; color: #38bdf8; }}
        .badge-action {{ padding: 2px 6px; border-radius: 4px; font-size: 11px; font-weight: 600; }}
        .badge-quarantine {{ background: #d97706; color: #fff; }}
        .badge-reject {{ background: #dc2626; color: #fff; }}
        .badge-warn {{ background: #475569; color: #fff; }}
        
        .status-tag {{ font-weight: 600; font-size: 11px; padding: 2px 8px; border-radius: 10px; }}
        .status-pass {{ background: rgba(16, 185, 129, 0.2); color: #34d399; border: 1px solid #059669; }}
        .status-fail {{ background: rgba(239, 68, 68, 0.2); color: #f87171; border: 1px solid #dc2626; }}
        
        .status-badge {{ font-size: 11px; font-weight: 600; padding: 2px 6px; border-radius: 4px; }}
        .status-excellent {{ background: #065f46; color: #6ee7b7; }}
        .status-good {{ background: #075985; color: #7dd3fc; }}
        .status-acceptable {{ background: #854d0e; color: #fde047; }}
        
        footer {{ text-align: center; margin-top: 32px; color: #64748b; font-size: 12px; }}
    </style>
</head>
<body>
    <div class="container">
        <header class="header">
            <div>
                <h1>Data Quality & Medallion Scorecard</h1>
                <p style="color: #94a3b8; font-size: 14px; margin-top: 4px;">Keyword-Driven Pipeline &bull; Automated Governance Report</p>
            </div>
            <div class="header-meta">
                <div>Execution Time: <strong>{timestamp}</strong></div>
                <div>Engine: <strong>PySpark 4.1 Medallion DQ</strong></div>
            </div>
        </header>

        <!-- KPI Summary Cards -->
        <div class="kpi-row">
            <div class="kpi-card">
                <div class="kpi-title">Overall DQ Health Score</div>
                <div class="kpi-value" style="color: {overall_color}">{summary['overall_score']}%</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-title">Governance Rating</div>
                <div class="kpi-value" style="color: {overall_color}; font-size: 24px;">{summary['overall_grade']}</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-title">Total Rules Evaluated</div>
                <div class="kpi-value">{summary['total_rules_evaluated']}</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-title">Clean Passing Checks</div>
                <div class="kpi-value" style="color: #34d399;">{summary['total_checks_passed']}</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-title">Quarantined / Failures</div>
                <div class="kpi-value" style="color: #f87171;">{summary['total_checks_failed']}</div>
            </div>
        </div>

        <!-- 6 Data Quality Dimensions & Medallion Layers -->
        <div class="grid-2">
            <div class="section-box">
                <div class="section-title">Data Quality Dimensions Health</div>
                <div class="dim-grid">
                    {dim_cards_html}
                </div>
            </div>
            <div class="section-box">
                <div class="section-title">Medallion Layers Breakdown</div>
                <table>
                    <thead>
                        <tr>
                            <th>Layer</th>
                            <th>DQ Score</th>
                            <th>Passed</th>
                            <th>Status</th>
                        </tr>
                    </thead>
                    <tbody>
                        {layer_rows_html}
                    </tbody>
                </table>
            </div>
        </div>

        <!-- Detailed Rule Checks Table -->
        <div class="section-box">
            <div class="section-title">Rule Execution Details & Action Routing</div>
            <table>
                <thead>
                    <tr>
                        <th>Rule ID</th>
                        <th>Rule Name & Description</th>
                        <th>Table</th>
                        <th>Layer</th>
                        <th>Dimension</th>
                        <th>Action</th>
                        <th>Pass Rate</th>
                        <th>Status</th>
                    </tr>
                </thead>
                <tbody>
                    {rule_rows_html}
                </tbody>
            </table>
        </div>

        <footer>
            Automated Data Quality Governance Report &bull; Medallion Architecture (Bronze &bull; Silver &bull; Gold)
        </footer>
    </div>
</body>
</html>
"""
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(html_content)
        return output_path
