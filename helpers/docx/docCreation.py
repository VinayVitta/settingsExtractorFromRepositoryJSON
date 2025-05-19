from docx import Document
from docx.shared import Inches, Pt, RGBColor
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
from datetime import datetime

def insert_field_code(run, field_code):
    """Insert a Word field code like PAGE or TOC."""
    fldChar1 = OxmlElement('w:fldChar')
    fldChar1.set(qn('w:fldCharType'), 'begin')

    instrText = OxmlElement('w:instrText')
    instrText.set(qn('xml:space'), 'preserve')
    instrText.text = field_code

    fldChar2 = OxmlElement('w:fldChar')
    fldChar2.set(qn('w:fldCharType'), 'separate')

    fldChar3 = OxmlElement('w:fldChar')
    fldChar3.set(qn('w:fldCharType'), 'end')

    run._r.append(fldChar1)
    run._r.append(instrText)
    run._r.append(fldChar2)
    run._r.append(fldChar3)


def export_tables_to_word(summary_tables, filename="task_summary.docx", title="Task Summary Report", logo_path="qlik_logo.png"):
    doc = Document()

    # --- Cover Page ---
    section = doc.sections[0]
    doc.add_picture(logo_path, width=Inches(1.2))
    doc.paragraphs[-1].alignment = WD_ALIGN_PARAGRAPH.RIGHT

    doc.add_paragraph("\n\n")
    p = doc.add_paragraph(title)
    p.style = 'Title'
    p.alignment = WD_ALIGN_PARAGRAPH.CENTER

    p = doc.add_paragraph(f"Date: {datetime.now().strftime('%B %d, %Y')}")
    p.alignment = WD_ALIGN_PARAGRAPH.CENTER

    p = doc.add_paragraph("Prepared by: Qlik Professional Services")
    p.alignment = WD_ALIGN_PARAGRAPH.CENTER

    doc.add_page_break()

    # --- Header with Logo ---
    for section in doc.sections:
        header = section.header
        header_p = header.paragraphs[0]
        run = header_p.add_run()
        try:
            run.add_picture(logo_path, width=Inches(1))
            header_p.alignment = WD_ALIGN_PARAGRAPH.RIGHT
        except Exception:
            pass  # Skip if logo not found

    # --- Footer with Page Number ---
    for section in doc.sections:
        footer = section.footer
        p = footer.paragraphs[0]
        p.alignment = WD_ALIGN_PARAGRAPH.CENTER
        run = p.add_run("Page ")
        insert_field_code(run, "PAGE")

    # --- TOC Placeholder ---
    doc.add_paragraph("Table of Contents", style='Heading 1')
    toc_paragraph = doc.add_paragraph()
    insert_field_code(toc_paragraph.add_run(), 'TOC \\o "1-3" \\h \\z \\u')
    doc.add_page_break()

    # --- Numbered Sections with Styled Headings and Text ---
    for idx, table_info in enumerate(summary_tables, 1):
        heading = doc.add_heading('', level=1)
        run = heading.add_run(f"{idx}. {table_info['title']}")
        run.font.name = 'Inter'
        run.font.size = Pt(18)
        run.font.bold = True
        run.font.color.rgb = RGBColor(0x00, 0x84, 0x3D)  # Qlik Green

        if "notes" in table_info:
            note_para = doc.add_paragraph()
            note_run = note_para.add_run(table_info["notes"])
            note_run.font.name = 'Inter'
            note_run.font.size = Pt(10.5)
            note_run.font.color.rgb = RGBColor(0x55, 0x5F, 0x68)  # Qlik Gray

        df = table_info["df"]
        if df.empty:
            empty_para = doc.add_paragraph("No data available.")
            empty_run = empty_para.runs[0]
            empty_run.font.name = 'Inter'
            empty_run.font.size = Pt(10.5)
            empty_run.font.color.rgb = RGBColor(0x55, 0x5F, 0x68)
            continue

        table = doc.add_table(rows=1, cols=len(df.columns))
        table.style = 'Light Grid'

        hdr_cells = table.rows[0].cells
        for i, col in enumerate(df.columns):
            cell = hdr_cells[i]
            run = cell.paragraphs[0].add_run(str(col))
            run.font.name = 'Inter'
            run.font.size = Pt(10.5)
            run.font.bold = True
            run.font.color.rgb = RGBColor(0x00, 0x84, 0x3D)  # Green headers

        for _, row in df.iterrows():
            row_cells = table.add_row().cells
            for i, col in enumerate(df.columns):
                run = row_cells[i].paragraphs[0].add_run(str(row[col]))
                run.font.name = 'Inter'
                run.font.size = Pt(10.5)
                run.font.color.rgb = RGBColor(0x55, 0x5F, 0x68)  # Qlik Gray

        doc.add_paragraph()

    doc.save(filename)
    print(f"📄 Exported to: {filename}")
