"""PDF rendering for evidence packages.

Uses ReportLab's Platypus flowables for layout. Output is a multi-page
PDF with sections: Title + summary, Cluster overview, Members table,
Linked agents, Fund flow summary, Top fund paths, Timeline.

The renderer is intentionally pure — it takes a payload dict and
returns ``(pdf_bytes, page_count)``. Storage / persistence happens
upstream in :mod:`core.evidence.builder`.
"""

from __future__ import annotations

import io
from datetime import UTC, datetime
from typing import Any

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.platypus import (
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)


def _styles() -> dict[str, ParagraphStyle]:
    base = getSampleStyleSheet()
    return {
        "title": ParagraphStyle(
            "EvidenceTitle",
            parent=base["Title"],
            fontSize=18,
            spaceAfter=10,
        ),
        "h1": ParagraphStyle(
            "EvidenceH1",
            parent=base["Heading1"],
            fontSize=13,
            spaceBefore=12,
            spaceAfter=6,
        ),
        "h2": ParagraphStyle(
            "EvidenceH2",
            parent=base["Heading2"],
            fontSize=11,
            spaceBefore=8,
            spaceAfter=4,
        ),
        "body": ParagraphStyle("EvidenceBody", parent=base["BodyText"], fontSize=9, leading=12),
        "small": ParagraphStyle("EvidenceSmall", parent=base["BodyText"], fontSize=8, leading=10),
    }


_TABLE_HEADER = TableStyle(
    [
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1f2937")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 8),
        ("BOTTOMPADDING", (0, 0), (-1, 0), 6),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f3f4f6")]),
        ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#9ca3af")),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
    ]
)


def _kv_block(rows: list[tuple[str, Any]], styles) -> Table:
    data = [[k, str(v if v is not None else "—")] for k, v in rows]
    table = Table(data, colWidths=[5 * cm, 11 * cm])
    table.setStyle(
        TableStyle(
            [
                ("FONTNAME", (0, 0), (0, -1), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, -1), 9),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
            ]
        )
    )
    return table


def _members_table(members: list[dict[str, Any]]) -> Table:
    data: list[list[Any]] = [["#", "ID", "Kind", "Role", "Confidence", "Risk", "Status"]]
    for i, m in enumerate(members, start=1):
        data.append(
            [
                i,
                str(m.get("id") or "—"),
                str(m.get("kind") or "—"),
                str(m.get("role") or "—"),
                f"{float(m.get('confidence') or 0):.2f}",
                f"{float(m.get('risk_score') or 0):.2f}",
                str(m.get("status") or "—"),
            ]
        )
    table = Table(
        data,
        colWidths=[1 * cm, 4 * cm, 2 * cm, 2.5 * cm, 2 * cm, 2 * cm, 2.5 * cm],
        repeatRows=1,
    )
    table.setStyle(_TABLE_HEADER)
    return table


def _agents_table(agents: list[dict[str, Any]]) -> Table:
    data: list[list[Any]] = [["Agent ID", "Name", "Area", "Class", "Risk", "Cashouts"]]
    for a in agents:
        data.append(
            [
                str(a.get("agent_id") or "—"),
                str(a.get("name") or "—")[:35],
                str(a.get("area") or "—"),
                str(a.get("classification") or "—"),
                f"{float(a.get('risk_score') or 0):.2f}",
                int(a.get("cashout_count") or 0),
            ]
        )
    table = Table(
        data,
        colWidths=[2.5 * cm, 4.5 * cm, 2.5 * cm, 2.5 * cm, 1.5 * cm, 2.5 * cm],
        repeatRows=1,
    )
    table.setStyle(_TABLE_HEADER)
    return table


def _paths_table(paths: list[dict[str, Any]]) -> Table:
    data: list[list[Any]] = [["Path", "Hops", "Bottleneck", "Cashout amt", "Agent", "Area"]]
    for p in paths:
        path_str = " → ".join(p.get("path") or [])[:60]
        data.append(
            [
                path_str,
                int(p.get("hops") or 0),
                f"GHS {float(p.get('bottleneck_amount') or 0):.0f}",
                f"GHS {float(p.get('cashout_amount') or 0):.0f}",
                str(p.get("cashout_agent_id") or "—"),
                str(p.get("cashout_area") or "—"),
            ]
        )
    table = Table(
        data,
        colWidths=[6 * cm, 1.2 * cm, 2.2 * cm, 2.4 * cm, 2.2 * cm, 2 * cm],
        repeatRows=1,
    )
    table.setStyle(_TABLE_HEADER)
    return table


def _timeline_table(events: list[dict[str, Any]], limit: int = 60) -> Table:
    data: list[list[Any]] = [["Timestamp", "Kind", "Description"]]
    for e in events[:limit]:
        data.append(
            [
                str(e.get("timestamp") or "—")[:25],
                str(e.get("kind") or "—"),
                str(e.get("description") or "—")[:90],
            ]
        )
    table = Table(
        data,
        colWidths=[4 * cm, 3 * cm, 9.5 * cm],
        repeatRows=1,
    )
    table.setStyle(_TABLE_HEADER)
    return table


def render_pdf(payload: dict[str, Any]) -> tuple[bytes, int]:
    """Render the assembled payload to a PDF and return ``(bytes, pages)``."""

    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=A4,
        title=f"FraudNet evidence — {payload['cluster'].get('cluster_id', '?')}",
        leftMargin=2 * cm,
        rightMargin=2 * cm,
        topMargin=2 * cm,
        bottomMargin=2 * cm,
    )
    styles = _styles()
    story: list[Any] = []

    cluster = payload["cluster"] or {}
    fund = payload["fund_flow"] or {}

    # --- Header --------------------------------------------------------
    story.append(
        Paragraph(
            f"FraudNet Evidence Package — {cluster.get('name') or cluster.get('cluster_id') or '?'}",
            styles["title"],
        )
    )
    story.append(
        Paragraph(
            f"Generated: {datetime.now(UTC).strftime('%Y-%m-%d %H:%M UTC')}",
            styles["small"],
        )
    )
    story.append(Spacer(1, 0.3 * cm))

    # --- Cluster overview ---------------------------------------------
    story.append(Paragraph("Cluster overview", styles["h1"]))
    story.append(
        _kv_block(
            [
                ("Cluster ID", cluster.get("cluster_id")),
                ("Name", cluster.get("name")),
                ("Status", cluster.get("status")),
                ("Confidence", f"{float(cluster.get('confidence_score') or 0):.3f}"),
                ("Estimated fraud value", f"GHS {float(cluster.get('estimated_fraud_value') or 0):.2f}"),
                ("Density", cluster.get("density")),
                ("Isolation", cluster.get("isolation_score")),
                ("Seed type", cluster.get("seed_type")),
                ("Seed node", cluster.get("seed_node_id")),
                ("Seed date", cluster.get("seed_date")),
                ("Member count", cluster.get("member_count")),
                ("Member kinds", ", ".join(cluster.get("member_labels") or []) or "—"),
            ],
            styles,
        )
    )
    story.append(Spacer(1, 0.3 * cm))

    # --- Members ------------------------------------------------------
    story.append(Paragraph(f"Members ({len(payload['members'])})", styles["h1"]))
    if payload["members"]:
        story.append(_members_table(payload["members"]))
    else:
        story.append(Paragraph("(no members recorded)", styles["body"]))
    story.append(PageBreak())

    # --- Linked agents -------------------------------------------------
    story.append(Paragraph(f"Linked cash-out agents ({len(payload['linked_agents'])})", styles["h1"]))
    if payload["linked_agents"]:
        story.append(_agents_table(payload["linked_agents"]))
    else:
        story.append(Paragraph("(no linked agents)", styles["body"]))

    story.append(Spacer(1, 0.4 * cm))

    # --- Fund flow summary --------------------------------------------
    story.append(Paragraph("Fund flow summary", styles["h1"]))
    story.append(
        _kv_block(
            [
                ("Seed wallets traced", ", ".join(fund.get("seed_wallets") or []) or "—"),
                ("Total traced value", f"GHS {float(fund.get('total_traced_value') or 0):.2f}"),
                ("Cash-out agents involved", len(fund.get("cashout_agents_summary") or [])),
            ],
            styles,
        )
    )
    story.append(Spacer(1, 0.3 * cm))

    # Top fund paths (flatten across seeds)
    flat_paths: list[dict[str, Any]] = []
    for paths in (fund.get("paths_per_seed") or {}).values():
        flat_paths.extend(paths)
    flat_paths.sort(key=lambda p: float(p.get("cashout_amount") or 0), reverse=True)
    if flat_paths:
        story.append(Paragraph("Top fund paths", styles["h2"]))
        story.append(_paths_table(flat_paths[:20]))
    story.append(PageBreak())

    # --- Timeline -----------------------------------------------------
    story.append(Paragraph(f"Event timeline ({len(payload['timeline'])} events)", styles["h1"]))
    if payload["timeline"]:
        story.append(_timeline_table(payload["timeline"]))
    else:
        story.append(Paragraph("(no events recorded)", styles["body"]))

    # Capture page count via a callback.
    page_counter = {"n": 0}

    def _on_page(_canvas, _doc):  # noqa: ANN001 — reportlab signature
        page_counter["n"] += 1

    doc.build(story, onFirstPage=_on_page, onLaterPages=_on_page)
    return buf.getvalue(), page_counter["n"]
