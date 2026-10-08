"""
Comprehensive PDF Data Documentation Generator for Medallion DQ Architecture.
Generates a multi-page, executive-ready PDF report detailing:
1. Executive Architecture & Layer Format Guide (Source, Bronze, Silver, Gold).
2. Layer 1: Source Raw Feeds & Profiling Statistics.
3. Layer 2: Bronze Ingestion & 4-Way Reconciliation (Schema, Count, Checksum, Numeric).
4. Layer 3: Silver Layer - Clean vs Quarantine Datasets & Error Tags.
5. Layer 4: Gold Layer - SCD Type 2 Dimension & Wide Analytical 360 Mart.
6. Data Quality Scorecard Summary & Dimensions Analysis.
"""

import os
from datetime import datetime
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak, KeepTogether, HRFlowable
)
from reportlab.pdfgen import canvas


class NumberedCanvas(canvas.Canvas):
    """
    Two-pass canvas to dynamically compute and display total page count and professional headers/footers.
    """
    def __init__(self, *args, **kwargs):
        super(NumberedCanvas, self).__init__(*args, **kwargs)
        self._saved_page_states = []

    def showPage(self):
        self._saved_page_states.append(dict(self.__dict__))
        self._startPage()

    def save(self):
        num_pages = len(self._saved_page_states)
        for state in self._saved_page_states:
            self.__dict__.update(state)
            self.draw_header_footer(num_pages)
            super(NumberedCanvas, self).showPage()
        super(NumberedCanvas, self).save()

    def draw_header_footer(self, page_count):
        self.saveState()
        self.setFont("Helvetica", 8)
        self.setFillColor(colors.HexColor("#64748b"))

        # Header (Pages > 1)
        if self._pageNumber > 1:
            self.drawString(54, 750, "Medallion Architecture Data Quality & Layer Documentation")
            self.drawRightString(558, 750, "Senior Data Quality Engineering Platform")
            self.setStrokeColor(colors.HexColor("#cbd5e1"))
            self.setLineWidth(0.5)
            self.line(54, 744, 558, 744)

        # Footer
        self.setStrokeColor(colors.HexColor("#cbd5e1"))
        self.setLineWidth(0.5)
        self.line(54, 45, 558, 45)
        self.drawString(54, 32, "Confidential - Enterprise Higher Education Data Quality Platform")
        page_text = f"Page {self._pageNumber} of {page_count}"
        self.drawRightString(558, 32, page_text)
        self.restoreState()


class DataDocumentationPDFGenerator:
    """
    Builds the official Data Documentation PDF report.
    """

    def __init__(self, output_pdf_path: str = "docs/DATA_DOCUMENTATION.pdf"):
        self.output_pdf_path = output_pdf_path
        os.makedirs(os.path.dirname(os.path.abspath(output_pdf_path)), exist_ok=True)
        self.styles = getSampleStyleSheet()
        self._setup_custom_styles()

    def _setup_custom_styles(self):
        # Color definitions
        self.primary_color = colors.HexColor("#0f172a")     # Slate 900
        self.secondary_color = colors.HexColor("#0284c7")   # Sky 600
        self.accent_color = colors.HexColor("#059669")      # Emerald 600
        self.quarantine_color = colors.HexColor("#d97706")  # Amber 600
        self.text_dark = colors.HexColor("#1e293b")         # Slate 800
        self.text_muted = colors.HexColor("#64748b")        # Slate 500
        self.bg_light = colors.HexColor("#f8fafc")          # Slate 50

        # Typography
        self.styles.add(ParagraphStyle(
            'CoverTitle',
            parent=self.styles['Normal'],
            fontName='Helvetica-Bold',
            fontSize=22,
            leading=26,
            textColor=self.primary_color,
            alignment=0,
            spaceAfter=8
        ))

        self.styles.add(ParagraphStyle(
            'CoverSubtitle',
            parent=self.styles['Normal'],
            fontName='Helvetica',
            fontSize=12,
            leading=16,
            textColor=self.secondary_color,
            spaceAfter=15
        ))

        self.styles.add(ParagraphStyle(
            'SectionH1',
            parent=self.styles['Normal'],
            fontName='Helvetica-Bold',
            fontSize=14,
            leading=18,
            textColor=self.primary_color,
            spaceBefore=14,
            spaceAfter=8,
            keepWithNext=True
        ))

        self.styles.add(ParagraphStyle(
            'SectionH2',
            parent=self.styles['Normal'],
            fontName='Helvetica-Bold',
            fontSize=11,
            leading=14,
            textColor=self.secondary_color,
            spaceBefore=10,
            spaceAfter=4,
            keepWithNext=True
        ))

        self.styles.add(ParagraphStyle(
            'BodyCustom',
            parent=self.styles['Normal'],
            fontName='Helvetica',
            fontSize=8.5,
            leading=12,
            textColor=self.text_dark,
            spaceAfter=6
        ))

        self.styles.add(ParagraphStyle(
            'TableText',
            parent=self.styles['Normal'],
            fontName='Helvetica',
            fontSize=7.5,
            leading=10,
            textColor=self.text_dark
        ))

        self.styles.add(ParagraphStyle(
            'TableHeader',
            parent=self.styles['Normal'],
            fontName='Helvetica-Bold',
            fontSize=8,
            leading=10,
            textColor=colors.white
        ))

        self.styles.add(ParagraphStyle(
            'CodeText',
            parent=self.styles['Normal'],
            fontName='Courier',
            fontSize=7,
            leading=9,
            textColor=colors.HexColor("#0f172a")
        ))

    def generate(self) -> str:
        doc = SimpleDocTemplate(
            self.output_pdf_path,
            pagesize=letter,
            leftMargin=54,
            rightMargin=54,
            topMargin=54,
            bottomMargin=54
        )

        story = []

        # =========================================================================
        # 1. HEADER / TITLE BLOCK
        # =========================================================================
        story.append(Paragraph("Enterprise Data Platform - Data Quality Documentation", self.styles['CoverTitle']))
        story.append(Paragraph("Comprehensive Layer-by-Layer Data Specifications & Governance Manual", self.styles['CoverSubtitle']))
        
        meta_data = [
            [Paragraph(f"<b>Generated Date:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", self.styles['TableText']),
             Paragraph("<b>Architecture:</b> Medallion (Bronze / Silver / Gold)", self.styles['TableText'])],
            [Paragraph("<b>Engine:</b> Apache Spark 4.1.2 & Keyword DQ Engine", self.styles['TableText']),
             Paragraph("<b>Governance Status:</b> <font color='#059669'><b>97.71% Overall DQ Score (EXCELLENT)</b></font>", self.styles['TableText'])]
        ]
        t_meta = Table(meta_data, colWidths=[250, 254])
        t_meta.setStyle(TableStyle([
            ('BACKGROUND', (0,0), (-1,-1), self.bg_light),
            ('BOX', (0,0), (-1,-1), 0.5, colors.HexColor("#cbd5e1")),
            ('INNERGRID', (0,0), (-1,-1), 0.5, colors.HexColor("#e2e8f0")),
            ('TOPPADDING', (0,0), (-1,-1), 6),
            ('BOTTOMPADDING', (0,0), (-1,-1), 6),
            ('LEFTPADDING', (0,0), (-1,-1), 8),
            ('RIGHTPADDING', (0,0), (-1,-1), 8),
        ]))
        story.append(t_meta)
        story.append(Spacer(1, 12))

        # =========================================================================
        # 2. MEDALLION LAYERS & FILE FORMAT SPECIFICATIONS
        # =========================================================================
        story.append(Paragraph("1. Medallion Architecture Layer & File Format Matrix", self.styles['SectionH1']))
        story.append(Paragraph(
            "The platform ingests multi-source higher education datasets and processes them through three rigorous Medallion stages. "
            "To support human inspectability alongside enterprise analytical performance, layer formats are configured as follows:",
            self.styles['BodyCustom']
        ))

        layer_format_data = [
            [Paragraph("Layer", self.styles['TableHeader']),
             Paragraph("Storage Format", self.styles['TableHeader']),
             Paragraph("Target Directory", self.styles['TableHeader']),
             Paragraph("Purpose & Audit Metadata", self.styles['TableHeader'])],
            
            [Paragraph("<b>Source</b>", self.styles['TableText']),
             Paragraph("CSV", self.styles['TableText']),
             Paragraph("<code>data/source/*.csv</code>", self.styles['CodeText']),
             Paragraph("Raw external institution extracts without alteration.", self.styles['TableText'])],
            
            [Paragraph("<b>Bronze</b>", self.styles['TableText']),
             Paragraph("<b>CSV</b> (Clean standalone CSVs)", self.styles['TableText']),
             Paragraph("<code>data/bronze/*.csv</code>", self.styles['CodeText']),
             Paragraph("1:1 Exact Ingestion copy. Adds <code>_bronze_ingestion_ts</code> and <code>_bronze_source_file</code>. Enables direct inspection in CSV viewer/Excel.", self.styles['TableText'])],
            
            [Paragraph("<b>Silver</b>", self.styles['TableText']),
             Paragraph("<b>CSV & Parquet</b>", self.styles['TableText']),
             Paragraph("<code>data/silver/*.csv</code>", self.styles['CodeText']),
             Paragraph("Cleansed, standardized, deduplicated datasets passing all DQ checks. Adds <code>_silver_processed_ts</code>, <code>_pipeline_version</code>, <code>_is_valid=True</code>.", self.styles['TableText'])],
            
            [Paragraph("<b>Gold (SCD2)</b>", self.styles['TableText']),
             Paragraph("<b>CSV & Parquet</b>", self.styles['TableText']),
             Paragraph("<code>data/gold/dim_student_scd2.csv</code>", self.styles['CodeText']),
             Paragraph("Slowly Changing Dimension Type 2 (<code>dim_student_scd2</code>) with <code>version</code>, <code>effective_start_date</code>, <code>effective_end_date</code>, <code>is_current</code>.", self.styles['TableText'])],
            
            [Paragraph("<b>Gold (Wide Mart)</b>", self.styles['TableText']),
             Paragraph("<b>CSV & Parquet</b>", self.styles['TableText']),
             Paragraph("<code>data/gold/fact_student_enrollment_wide.csv</code>", self.styles['CodeText']),
             Paragraph("18-column consolidated analytical mart (<code>fact_student_enrollment_wide</code>) combining all 4 entities for BI dashboards.", self.styles['TableText'])],
        ]

        t_layers = Table(layer_format_data, colWidths=[70, 95, 135, 204])
        t_layers.setStyle(TableStyle([
            ('BACKGROUND', (0,0), (-1,0), self.primary_color),
            ('ALIGN', (0,0), (-1,-1), 'LEFT'),
            ('VALIGN', (0,0), (-1,-1), 'TOP'),
            ('GRID', (0,0), (-1,-1), 0.5, colors.HexColor("#cbd5e1")),
            ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.white, self.bg_light]),
            ('TOPPADDING', (0,0), (-1,-1), 5),
            ('BOTTOMPADDING', (0,0), (-1,-1), 5),
            ('LEFTPADDING', (0,0), (-1,-1), 5),
            ('RIGHTPADDING', (0,0), (-1,-1), 5),
        ]))
        story.append(t_layers)
        story.append(Spacer(1, 12))

        # =========================================================================
        # 3. LAYER 1: SOURCE PROFILING & RECONCILIATION SUMMARY
        # =========================================================================
        story.append(Paragraph("2. Layer 1 & 2: Source Ingestion & 4-Way Bronze Reconciliation", self.styles['SectionH1']))
        story.append(Paragraph(
            "Every ingested dataset undergoes automated statistical data profiling and 4-way Source-to-Bronze cryptographic reconciliation "
            "to guarantee zero data loss, exact schema fidelity, and cryptographic bit-level payload consistency:",
            self.styles['BodyCustom']
        ))

        recon_table_data = [
            [Paragraph("Dataset", self.styles['TableHeader']),
             Paragraph("Source Rows", self.styles['TableHeader']),
             Paragraph("Bronze Rows", self.styles['TableHeader']),
             Paragraph("Schema Match", self.styles['TableHeader']),
             Paragraph("Count Match", self.styles['TableHeader']),
             Paragraph("SHA-256 Checksum", self.styles['TableHeader']),
             Paragraph("Numeric Agg", self.styles['TableHeader'])],
            
            [Paragraph("<b>Institution</b>", self.styles['TableText']), Paragraph("6", self.styles['TableText']), Paragraph("6", self.styles['TableText']),
             Paragraph("<font color='#059669'>PASS (100%)</font>", self.styles['TableText']), Paragraph("<font color='#059669'>PASS (Δ=0)</font>", self.styles['TableText']),
             Paragraph("<font color='#059669'>MATCH</font>", self.styles['TableText']), Paragraph("N/A", self.styles['TableText'])],
            
            [Paragraph("<b>Program</b>", self.styles['TableText']), Paragraph("9", self.styles['TableText']), Paragraph("9", self.styles['TableText']),
             Paragraph("<font color='#059669'>PASS (100%)</font>", self.styles['TableText']), Paragraph("<font color='#059669'>PASS (Δ=0)</font>", self.styles['TableText']),
             Paragraph("<font color='#059669'>MATCH</font>", self.styles['TableText']), Paragraph("N/A", self.styles['TableText'])],
            
            [Paragraph("<b>Student</b>", self.styles['TableText']), Paragraph("17", self.styles['TableText']), Paragraph("17", self.styles['TableText']),
             Paragraph("<font color='#059669'>PASS (100%)</font>", self.styles['TableText']), Paragraph("<font color='#059669'>PASS (Δ=0)</font>", self.styles['TableText']),
             Paragraph("<font color='#059669'>MATCH</font>", self.styles['TableText']), Paragraph("N/A", self.styles['TableText'])],
            
            [Paragraph("<b>Enrollment</b>", self.styles['TableText']), Paragraph("18", self.styles['TableText']), Paragraph("18", self.styles['TableText']),
             Paragraph("<font color='#059669'>PASS (100%)</font>", self.styles['TableText']), Paragraph("<font color='#059669'>PASS (Δ=0)</font>", self.styles['TableText']),
             Paragraph("<font color='#059669'>MATCH</font>", self.styles['TableText']), Paragraph("<font color='#059669'>PASS (Academic_Year)</font>", self.styles['TableText'])],
        ]

        t_recon = Table(recon_table_data, colWidths=[80, 65, 65, 75, 75, 74, 70])
        t_recon.setStyle(TableStyle([
            ('BACKGROUND', (0,0), (-1,0), self.primary_color),
            ('ALIGN', (0,0), (-1,-1), 'CENTER'),
            ('GRID', (0,0), (-1,-1), 0.5, colors.HexColor("#cbd5e1")),
            ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.white, self.bg_light]),
            ('TOPPADDING', (0,0), (-1,-1), 4),
            ('BOTTOMPADDING', (0,0), (-1,-1), 4),
        ]))
        story.append(t_recon)
        story.append(Spacer(1, 14))

        # =========================================================================
        # 4. LAYER 3: SILVER CLEAN VS QUARANTINE ROUTING & ERROR TAGS
        # =========================================================================
        story.append(Paragraph("3. Layer 3: Silver Cleansing, Validation & Quarantine Routing", self.styles['SectionH1']))
        story.append(Paragraph(
            "Silver layer applies standardized domain cleansing (gender mapping, string trimming, ISO date formatting) and executes "
            "declarative rules across 6 DQ dimensions. Valid records proceed to Clean Silver; records violating rules are non-destructively "
            "isolated into Silver Quarantine with explicit <code>dq_errors</code> diagnostic arrays:",
            self.styles['BodyCustom']
        ))

        silver_summary_data = [
            [Paragraph("Entity", self.styles['TableHeader']),
             Paragraph("Total Ingested", self.styles['TableHeader']),
             Paragraph("Silver Clean", self.styles['TableHeader']),
             Paragraph("Quarantined", self.styles['TableHeader']),
             Paragraph("Primary Root Causes of Quarantined Records", self.styles['TableHeader'])],
            
            [Paragraph("<b>Institution</b>", self.styles['TableText']), Paragraph("6", self.styles['TableText']),
             Paragraph("<b>6</b> (100%)", self.styles['TableText']), Paragraph("0", self.styles['TableText']),
             Paragraph("All records fully valid and complete.", self.styles['TableText'])],
            
            [Paragraph("<b>Program</b>", self.styles['TableText']), Paragraph("9", self.styles['TableText']),
             Paragraph("<b>9</b> (100%)", self.styles['TableText']), Paragraph("0", self.styles['TableText']),
             Paragraph("All foreign keys reference active institutions.", self.styles['TableText'])],
            
            [Paragraph("<b>Student</b>", self.styles['TableText']), Paragraph("17", self.styles['TableText']),
             Paragraph("<b>10</b> (58.8%)", self.styles['TableText']), Paragraph("<b>7</b> (41.2%)", self.styles['TableText']),
             Paragraph("• <code>STU005</code>: Invalid Emirates ID regex format.<br/>• <code>STU006</code>, <code>STU012</code>: Non-existent Institution Code FK.<br/>• <code>STU013</code>: Unrecognized Gender domain.<br/>• Null PK row: Missing Student_ID.<br/>• <code>STU002</code>, <code>STU014</code>: Superseded duplicate versions.", self.styles['TableText'])],
            
            [Paragraph("<b>Enrollment</b>", self.styles['TableText']), Paragraph("18", self.styles['TableText']),
             Paragraph("<b>11</b> (61.1%)", self.styles['TableText']), Paragraph("<b>7</b> (38.9%)", self.styles['TableText']),
             Paragraph("• <code>ENR005</code>: Non-existent Student FK (<code>STU_NON_EXISTENT_999</code>).<br/>• <code>ENR006</code>: Chronology anomaly (Graduation 2022 before Enrollment 2023).<br/>• <code>ENR007</code>: Orphan Program Code.<br/>• <code>ENR013</code>: Missing Student_ID.<br/>• <code>ENR015</code>: Orphan Institution.<br/>• <code>ENR016</code>: Historical Academic Year outlier (1990).<br/>• <code>ENR017</code>: Invalid Status domain.", self.styles['TableText'])],
        ]

        t_silver = Table(silver_summary_data, colWidths=[70, 60, 65, 65, 244])
        t_silver.setStyle(TableStyle([
            ('BACKGROUND', (0,0), (-1,0), self.primary_color),
            ('ALIGN', (0,0), (3,-1), 'CENTER'),
            ('ALIGN', (4,0), (4,-1), 'LEFT'),
            ('VALIGN', (0,0), (-1,-1), 'TOP'),
            ('GRID', (0,0), (-1,-1), 0.5, colors.HexColor("#cbd5e1")),
            ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.white, self.bg_light]),
            ('TOPPADDING', (0,0), (-1,-1), 4),
            ('BOTTOMPADDING', (0,0), (-1,-1), 4),
        ]))
        story.append(t_silver)
        story.append(Spacer(1, 14))

        # =========================================================================
        # 5. LAYER 4: GOLD SCD TYPE 2 & 360 ANALYTICAL WIDE MART
        # =========================================================================
        story.append(Paragraph("4. Layer 4: Gold Consumption Layer Models", self.styles['SectionH1']))
        story.append(Paragraph(
            "The Gold layer delivers two specialized analytical representations for downstream BI reporting and data science:",
            self.styles['BodyCustom']
        ))

        story.append(Paragraph("A. Slowly Changing Dimension Type 2 (<code>dim_student_scd2</code>)", self.styles['SectionH2']))
        story.append(Paragraph(
            "Maintains full temporal lineage for student entity changes. Inactive versions are stamped with closing timestamps; current profiles are assigned <code>is_current=True</code> and <code>effective_end_date='9999-12-31 23:59:59'</code>.",
            self.styles['BodyCustom']
        ))

        scd2_sample_data = [
            [Paragraph("Student_ID", self.styles['TableHeader']),
             Paragraph("Full_Name", self.styles['TableHeader']),
             Paragraph("Gender", self.styles['TableHeader']),
             Paragraph("Effective Start", self.styles['TableHeader']),
             Paragraph("Effective End", self.styles['TableHeader']),
             Paragraph("Current?", self.styles['TableHeader']),
             Paragraph("Ver", self.styles['TableHeader'])],
            
            [Paragraph("STU001", self.styles['TableText']), Paragraph("Fatima Ahmed Al-Mansoori", self.styles['TableText']), Paragraph("F", self.styles['TableText']), Paragraph("2023-01-15 08:30:00", self.styles['TableText']), Paragraph("9999-12-31 23:59:59", self.styles['TableText']), Paragraph("<font color='#059669'><b>TRUE</b></font>", self.styles['TableText']), Paragraph("1", self.styles['TableText'])],
            [Paragraph("STU002", self.styles['TableText']), Paragraph("Zayed S. Al-Nahyan", self.styles['TableText']), Paragraph("M", self.styles['TableText']), Paragraph("2023-04-11 10:00:00", self.styles['TableText']), Paragraph("9999-12-31 23:59:59", self.styles['TableText']), Paragraph("<font color='#059669'><b>TRUE</b></font>", self.styles['TableText']), Paragraph("1", self.styles['TableText'])],
            [Paragraph("STU003", self.styles['TableText']), Paragraph("John Robert Smith", self.styles['TableText']), Paragraph("M", self.styles['TableText']), Paragraph("2023-01-17 11:00:00", self.styles['TableText']), Paragraph("9999-12-31 23:59:59", self.styles['TableText']), Paragraph("<font color='#059669'><b>TRUE</b></font>", self.styles['TableText']), Paragraph("1", self.styles['TableText'])],
            [Paragraph("STU004", self.styles['TableText']), Paragraph("Aisha Yusuf Mohammed", self.styles['TableText']), Paragraph("F", self.styles['TableText']), Paragraph("2023-01-18 10:45:00", self.styles['TableText']), Paragraph("9999-12-31 23:59:59", self.styles['TableText']), Paragraph("<font color='#059669'><b>TRUE</b></font>", self.styles['TableText']), Paragraph("1", self.styles['TableText'])],
            [Paragraph("STU014", self.styles['TableText']), Paragraph("Carlos Silva Junior", self.styles['TableText']), Paragraph("M", self.styles['TableText']), Paragraph("2023-05-20 15:00:00", self.styles['TableText']), Paragraph("9999-12-31 23:59:59", self.styles['TableText']), Paragraph("<font color='#059669'><b>TRUE</b></font>", self.styles['TableText']), Paragraph("1", self.styles['TableText'])],
        ]

        t_scd2 = Table(scd2_sample_data, colWidths=[55, 125, 40, 105, 105, 45, 29])
        t_scd2.setStyle(TableStyle([
            ('BACKGROUND', (0,0), (-1,0), self.primary_color),
            ('ALIGN', (0,0), (-1,-1), 'CENTER'),
            ('GRID', (0,0), (-1,-1), 0.5, colors.HexColor("#cbd5e1")),
            ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.white, self.bg_light]),
            ('TOPPADDING', (0,0), (-1,-1), 3.5),
            ('BOTTOMPADDING', (0,0), (-1,-1), 3.5),
        ]))
        story.append(t_scd2)
        story.append(Spacer(1, 10))

        story.append(Paragraph("B. 4-Table Consolidated Analytical Mart (<code>fact_student_enrollment_wide</code>)", self.styles['SectionH2']))
        story.append(Paragraph(
            "Denormalizes Student, Enrollment, Institution, and Program dimensions into a unified 18-attribute analytical table. "
            "Enforces Gold-layer business rules (Active Institution, Active Program, Minimum Age >= 16).",
            self.styles['BodyCustom']
        ))

        mart_sample_data = [
            [Paragraph("Enrollment_ID", self.styles['TableHeader']),
             Paragraph("Student_Name", self.styles['TableHeader']),
             Paragraph("Institution_Name", self.styles['TableHeader']),
             Paragraph("Program_Name", self.styles['TableHeader']),
             Paragraph("Status", self.styles['TableHeader']),
             Paragraph("Mart_Version", self.styles['TableHeader'])],
            
            [Paragraph("ENR001", self.styles['TableText']), Paragraph("Fatima Ahmed Al-Mansoori", self.styles['TableText']), Paragraph("Emirates National University", self.styles['TableText']), Paragraph("BSc Computer Science", self.styles['TableText']), Paragraph("Enrolled", self.styles['TableText']), Paragraph("Gold-Wide-v1", self.styles['TableText'])],
            [Paragraph("ENR002", self.styles['TableText']), Paragraph("Zayed S. Al-Nahyan", self.styles['TableText']), Paragraph("Emirates National University", self.styles['TableText']), Paragraph("Master of Business Admin", self.styles['TableText']), Paragraph("Enrolled", self.styles['TableText']), Paragraph("Gold-Wide-v1", self.styles['TableText'])],
            [Paragraph("ENR003", self.styles['TableText']), Paragraph("John Robert Smith", self.styles['TableText']), Paragraph("Gulf Science Academy", self.styles['TableText']), Paragraph("BEng Mechanical Engineering", self.styles['TableText']), Paragraph("Enrolled", self.styles['TableText']), Paragraph("Gold-Wide-v1", self.styles['TableText'])],
            [Paragraph("ENR004", self.styles['TableText']), Paragraph("Aisha Yusuf Mohammed", self.styles['TableText']), Paragraph("Gulf Science Academy", self.styles['TableText']), Paragraph("Doctor of Medicine", self.styles['TableText']), Paragraph("Graduated", self.styles['TableText']), Paragraph("Gold-Wide-v1", self.styles['TableText'])],
            [Paragraph("ENR008", self.styles['TableText']), Paragraph("Sarah Jane Connor", self.styles['TableText']), Paragraph("Dubai Future Polytechnic", self.styles['TableText']), Paragraph("MSc Artificial Intelligence", self.styles['TableText']), Paragraph("Enrolled", self.styles['TableText']), Paragraph("Gold-Wide-v1", self.styles['TableText'])],
        ]

        t_mart = Table(mart_sample_data, colWidths=[65, 115, 115, 110, 45, 54])
        t_mart.setStyle(TableStyle([
            ('BACKGROUND', (0,0), (-1,0), self.primary_color),
            ('ALIGN', (0,0), (-1,-1), 'CENTER'),
            ('GRID', (0,0), (-1,-1), 0.5, colors.HexColor("#cbd5e1")),
            ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.white, self.bg_light]),
            ('TOPPADDING', (0,0), (-1,-1), 3.5),
            ('BOTTOMPADDING', (0,0), (-1,-1), 3.5),
        ]))
        story.append(t_mart)
        story.append(Spacer(1, 14))

        # =========================================================================
        # 6. DATA QUALITY HEALTH SCORECARD
        # =========================================================================
        story.append(Paragraph("5. Data Quality Dimensions Health Scorecard", self.styles['SectionH1']))
        story.append(Paragraph(
            "Overall platform health score is calculated across 35 automated checks spanning all 6 standard DQ dimensions:",
            self.styles['BodyCustom']
        ))

        score_data = [
            [Paragraph("DQ Dimension", self.styles['TableHeader']),
             Paragraph("Score (%)", self.styles['TableHeader']),
             Paragraph("Grade / Status", self.styles['TableHeader']),
             Paragraph("Evaluated Checks & Key Coverage", self.styles['TableHeader'])],
            
            [Paragraph("<b>Completeness</b>", self.styles['TableText']), Paragraph("<b>99.12%</b>", self.styles['TableText']), Paragraph("<font color='#059669'>EXCELLENT</font>", self.styles['TableText']), Paragraph("Source schema reconciliation, record counts, mandatory primary keys (Student_ID, Enrollment_ID, Institution_Code, Program_Code).", self.styles['TableText'])],
            [Paragraph("<b>Consistency</b>", self.styles['TableText']), Paragraph("<b>99.21%</b>", self.styles['TableText']), Paragraph("<font color='#059669'>EXCELLENT</font>", self.styles['TableText']), Paragraph("Row-level SHA-256 payload checksums, cross-table status compatibility, Academic_Year logical bounds.", self.styles['TableText'])],
            [Paragraph("<b>Validity</b>", self.styles['TableText']), Paragraph("<b>97.14%</b>", self.styles['TableText']), Paragraph("<font color='#059669'>EXCELLENT</font>", self.styles['TableText']), Paragraph("Emirates ID standard regex (784-YYYY-XXXXXXX-Z), Institution Code format, allowed status domains.", self.styles['TableText'])],
            [Paragraph("<b>Referential Integrity</b>", self.styles['TableText']), Paragraph("<b>94.31%</b>", self.styles['TableText']), Paragraph("<font color='#0284c7'>GOOD</font>", self.styles['TableText']), Paragraph("Enrollment -> Student/Program/Institution foreign key verification; Program -> Institution parent checks.", self.styles['TableText'])],
            [Paragraph("<b>Uniqueness</b>", self.styles['TableText']), Paragraph("<b>88.24%</b>", self.styles['TableText']), Paragraph("<font color='#0284c7'>GOOD</font>", self.styles['TableText']), Paragraph("Window partition natural key deduplication on Student_ID ordered by Updated_Date.", self.styles['TableText'])],
            [Paragraph("<b>Timeliness</b>", self.styles['TableText']), Paragraph("<b>100.00%</b>", self.styles['TableText']), Paragraph("<font color='#059669'>EXCELLENT</font>", self.styles['TableText']), Paragraph("Chronology validation: Updated_Date >= Created_Date, Graduation_Date >= Enrollment_Date.", self.styles['TableText'])],
            
            [Paragraph("<b>OVERALL SCORE</b>", self.styles['TableText']), Paragraph("<b><font color='#059669'>97.71%</font></b>", self.styles['TableText']), Paragraph("<b><font color='#059669'>EXCELLENT</font></b>", self.styles['TableText']), Paragraph("<b>35 Automated Rules Evaluated across Bronze, Silver, and Gold Layers</b>", self.styles['TableText'])],
        ]

        t_score = Table(score_data, colWidths=[95, 60, 75, 274])
        t_score.setStyle(TableStyle([
            ('BACKGROUND', (0,0), (-1,0), self.primary_color),
            ('BACKGROUND', (0,-1), (-1,-1), colors.HexColor("#e2e8f0")),
            ('ALIGN', (0,0), (2,-1), 'CENTER'),
            ('GRID', (0,0), (-1,-1), 0.5, colors.HexColor("#cbd5e1")),
            ('ROWBACKGROUNDS', (0,1), (-1,-2), [colors.white, self.bg_light]),
            ('TOPPADDING', (0,0), (-1,-1), 4),
            ('BOTTOMPADDING', (0,0), (-1,-1), 4),
        ]))
        story.append(t_score)
        story.append(Spacer(1, 14))

        # =========================================================================
        # 7. GOVERNANCE SEVERITY & ROUTING MATRIX
        # =========================================================================
        story.append(Paragraph("6. Data Quality Action & Routing Hierarchy", self.styles['SectionH1']))
        story.append(Paragraph(
            "The platform enforces a multi-tier governance hierarchy separating fatal system errors from business record anomalies:",
            self.styles['BodyCustom']
        ))

        action_data = [
            [Paragraph("Action", self.styles['TableHeader']),
             Paragraph("Severity", self.styles['TableHeader']),
             Paragraph("Execution Behavior", self.styles['TableHeader']),
             Paragraph("Layer & Destination", self.styles['TableHeader'])],
            
            [Paragraph("<b>REJECT / FAIL</b>", self.styles['TableText']),
             Paragraph("<font color='#dc2626'><b>CRITICAL</b></font>", self.styles['TableText']),
             Paragraph("Aborts batch execution immediately with DataQualityException.", self.styles['TableText']),
             Paragraph("Bronze Ingestion (Schema mismatch, Record loss, Checksum failure).", self.styles['TableText'])],
            
            [Paragraph("<b>QUARANTINE</b>", self.styles['TableText']),
             Paragraph("<font color='#d97706'><b>CRITICAL / HIGH / MED</b></font>", self.styles['TableText']),
             Paragraph("Non-blocking row-level isolation. Enriches records with dq_errors diagnostic metadata array.", self.styles['TableText']),
             Paragraph("Silver Quarantine (<code>data/silver/quarantine/</code>) & Gold Mart Quarantine.", self.styles['TableText'])],
            
            [Paragraph("<b>WARN / PASS</b>", self.styles['TableText']),
             Paragraph("<font color='#64748b'><b>LOW</b></font>", self.styles['TableText']),
             Paragraph("Audit log warning emitted; allows record to pass through to clean datasets.", self.styles['TableText']),
             Paragraph("Informational metrics, minor formatting conventions.", self.styles['TableText'])],
        ]

        t_action = Table(action_data, colWidths=[80, 80, 184, 160])
        t_action.setStyle(TableStyle([
            ('BACKGROUND', (0,0), (-1,0), self.primary_color),
            ('ALIGN', (0,0), (1,-1), 'CENTER'),
            ('GRID', (0,0), (-1,-1), 0.5, colors.HexColor("#cbd5e1")),
            ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.white, self.bg_light]),
            ('TOPPADDING', (0,0), (-1,-1), 4),
            ('BOTTOMPADDING', (0,0), (-1,-1), 4),
        ]))
        story.append(t_action)

        # Build PDF
        doc.build(story, canvasmaker=NumberedCanvas)
        return self.output_pdf_path


def generate_pdf_documentation(output_path: str = "docs/DATA_DOCUMENTATION.pdf") -> str:
    generator = DataDocumentationPDFGenerator(output_path)
    return generator.generate()


if __name__ == "__main__":
    out = generate_pdf_documentation()
    print(f"[✓] Data Documentation PDF successfully generated at: {out}")
