import io
import hashlib
import re
import uuid
import zipfile
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import pandas as pd
import qrcode
import streamlit as st
from pypdf import PdfReader, PdfWriter
from rapidfuzz import fuzz
from reportlab.lib.utils import ImageReader
from reportlab.pdfgen import canvas


st.set_page_config(page_title="Certificate Management System", layout="wide")


HEADER_STYLE = """
<style>
.main-title {
    font-size: 2rem;
    font-weight: 700;
    color: #2D1B69;
    margin-bottom: 0.2rem;
}
.sub-title {
    color: #5F5F7A;
    margin-bottom: 1rem;
}
.stTabs [data-baseweb="tab-list"] {
    gap: 0.4rem;
}
.stTabs [data-baseweb="tab"] {
    border-radius: 10px 10px 0 0;
    padding: 0.6rem 0.8rem;
}
</style>
"""
st.markdown(HEADER_STYLE, unsafe_allow_html=True)


def normalize_text(value: str) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip().lower()
    text = re.sub(r"[^a-z0-9\s@._-]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def normalize_email(value: str) -> str:
    email = normalize_text(value)
    return email.replace(" ", "")


def normalize_name_for_match(value: str) -> str:
    text = normalize_text(value)
    text = text.replace(",", " ")
    text = text.replace(".", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text.upper()


def safe_text(value) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).strip()


def title_case_name(name: str) -> str:
    return " ".join(piece.capitalize() for piece in normalize_name_for_match(name).split())


def detect_column(columns: List[str], candidates: List[str]) -> Optional[str]:
    normalized_map = {c: normalize_text(c).replace(" ", "") for c in columns}
    candidate_set = [normalize_text(c).replace(" ", "") for c in candidates]

    for original, normalized in normalized_map.items():
        if normalized in candidate_set:
            return original

    for original, normalized in normalized_map.items():
        for key in candidate_set:
            if key in normalized or normalized in key:
                return original
    return None


@dataclass
class ParsedParticipant:
    email: str
    full_name: str
    first_name: str
    middle_name: str
    last_name: str
    source_row: int
    source: str
    notes: str = ""

    @property
    def canonical_name(self) -> str:
        return normalize_name_for_match(self.full_name)


EMAIL_COLUMNS = [
    "email",
    "email address",
    "e-mail",
    "mail",
    "participant email",
]
FIRST_COLUMNS = ["first name", "firstname", "given name", "fname", "first"]
MIDDLE_COLUMNS = ["middle name", "middlename", "mname", "middle initial", "mi"]
LAST_COLUMNS = ["last name", "lastname", "surname", "family name", "lname", "last"]
FULLNAME_COLUMNS = ["full name", "name", "participant name", "complete name"]
STATUS_COLUMNS = ["status", "completed", "completion", "submitted", "attendance status"]
TIMESTAMP_COLUMNS = ["timestamp", "date submitted", "submission time", "time submitted", "time"]


def split_name_fallback(full_name: str) -> Tuple[str, str, str]:
    parts = [p for p in re.split(r"\s+", normalize_name_for_match(full_name)) if p]
    if not parts:
        return "", "", ""
    if len(parts) == 1:
        return parts[0], "", ""
    if len(parts) == 2:
        return parts[0], "", parts[1]
    return parts[0], " ".join(parts[1:-1]), parts[-1]


def assemble_full_name(first: str, middle: str, last: str, uppercase_output: bool) -> str:
    raw = " ".join([piece.strip() for piece in [first, middle, last] if piece and piece.strip()])
    clean = re.sub(r"\s+", " ", raw).strip()
    if not clean:
        return ""
    return normalize_name_for_match(clean) if uppercase_output else title_case_name(clean)


def clean_text_cell(value):
    if isinstance(value, str):
        value = value.replace("\xa0", " ")
        value = re.sub(r"\s+", " ", value).strip()
        return value
    return value


def clean_uploaded_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    cleaned = df.copy()
    cleaned.columns = [clean_text_cell(str(col)) for col in cleaned.columns]
    for col in cleaned.select_dtypes(include=["object"]).columns:
        cleaned[col] = cleaned[col].map(clean_text_cell)
    return cleaned


def analyze_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    audit = df.copy()
    audit["_dup_email_key"] = audit["Email Address"].fillna("").map(normalize_email)
    audit["_dup_name_key"] = audit["Full Name"].fillna("").map(normalize_name_for_match)

    email_dupe_mask = (audit["_dup_email_key"].str.len() > 0) & (
        audit["_dup_email_key"].duplicated(keep=False)
    )
    name_dupe_mask = (audit["_dup_name_key"].str.len() > 0) & (
        audit["_dup_name_key"].duplicated(keep=False)
    )

    dupes = audit[email_dupe_mask | name_dupe_mask].copy()
    dupes["Duplicate Reason"] = ""
    dupes.loc[email_dupe_mask & name_dupe_mask, "Duplicate Reason"] = (
        "Duplicate email and duplicate name"
    )
    dupes.loc[email_dupe_mask & ~name_dupe_mask, "Duplicate Reason"] = (
        "Duplicate email (different/same name)"
    )
    dupes.loc[~email_dupe_mask & name_dupe_mask, "Duplicate Reason"] = (
        "Duplicate name (different/same email)"
    )
    dupes = dupes.drop(columns=["_dup_email_key", "_dup_name_key"])
    dupes = dupes.sort_values(by=["Duplicate Reason", "Full Name", "Email Address"]).reset_index(drop=True)
    return dupes


def parse_uploaded_csv(df: pd.DataFrame, source_name: str, uppercase_output: bool) -> Tuple[pd.DataFrame, Dict[str, Optional[str]]]:
    df = clean_uploaded_dataframe(df)
    columns = list(df.columns)
    email_col = detect_column(columns, EMAIL_COLUMNS)
    first_col = detect_column(columns, FIRST_COLUMNS)
    middle_col = detect_column(columns, MIDDLE_COLUMNS)
    last_col = detect_column(columns, LAST_COLUMNS)
    full_col = detect_column(columns, FULLNAME_COLUMNS)
    status_col = detect_column(columns, STATUS_COLUMNS)
    timestamp_col = detect_column(columns, TIMESTAMP_COLUMNS)

    detected = {
        "email": email_col,
        "first_name": first_col,
        "middle_name": middle_col,
        "last_name": last_col,
        "full_name": full_col,
        "status": status_col,
        "timestamp": timestamp_col,
    }

    parsed_rows: List[ParsedParticipant] = []
    for idx, row in df.iterrows():
        email = normalize_email(row[email_col]) if email_col and email_col in row else ""
        first = normalize_name_for_match(row[first_col]) if first_col and first_col in row and not pd.isna(row[first_col]) else ""
        middle = normalize_name_for_match(row[middle_col]) if middle_col and middle_col in row and not pd.isna(row[middle_col]) else ""
        last = normalize_name_for_match(row[last_col]) if last_col and last_col in row and not pd.isna(row[last_col]) else ""

        if not (first or middle or last):
            full_raw = str(row[full_col]) if full_col and full_col in row and not pd.isna(row[full_col]) else ""
            first, middle, last = split_name_fallback(full_raw)

        full_name = assemble_full_name(first, middle, last, uppercase_output)
        if not full_name and full_col and full_col in row and not pd.isna(row[full_col]):
            first_fallback, middle_fallback, last_fallback = split_name_fallback(str(row[full_col]))
            full_name = assemble_full_name(first_fallback, middle_fallback, last_fallback, uppercase_output)
        notes = []
        if not email:
            notes.append("Missing email")
        if not full_name:
            notes.append("Missing name")
        status_val = str(row[status_col]).strip() if status_col and status_col in row and not pd.isna(row[status_col]) else ""
        if status_col and not status_val:
            notes.append("Empty status")

        parsed_rows.append(
            ParsedParticipant(
                email=email,
                full_name=full_name,
                first_name=first,
                middle_name=middle,
                last_name=last,
                source_row=int(idx) + 2,
                source=source_name,
                notes="; ".join(notes),
            )
        )

    out = pd.DataFrame(
        [
            {
                "Email Address": p.email,
                "Full Name": p.full_name,
                "First Name": p.first_name,
                "Middle Name": p.middle_name,
                "Last Name": p.last_name,
                "Canonical Name": p.canonical_name,
                "Source": p.source,
                "Source Row": p.source_row,
                "Notes": p.notes,
            }
            for p in parsed_rows
        ]
    )

    if timestamp_col:
        out["Timestamp"] = df[timestamp_col].astype(str)
    else:
        out["Timestamp"] = ""

    out["Status Raw"] = df[status_col].astype(str) if status_col else ""
    return out, detected


def deduplicate_participants(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    working = df.copy()
    working["Dedup Key"] = working["Email Address"].where(
        working["Email Address"].str.len() > 0, working["Canonical Name"]
    )
    working["Dedup Key"] = working["Dedup Key"].fillna("")

    dupes = analyze_duplicates(working)

    deduped = working.drop_duplicates(subset=["Dedup Key"], keep="first").copy()
    return deduped, dupes


def build_master_list(att: pd.DataFrame, eva: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    att_keyed = att.copy()
    eva_keyed = eva.copy()

    att_keyed["Key"] = att_keyed["Email Address"].where(
        att_keyed["Email Address"].str.len() > 0, att_keyed["Canonical Name"]
    )
    eva_keyed["Key"] = eva_keyed["Email Address"].where(
        eva_keyed["Email Address"].str.len() > 0, eva_keyed["Canonical Name"]
    )

    att_keyed = att_keyed[att_keyed["Key"] != ""]
    eva_keyed = eva_keyed[eva_keyed["Key"] != ""]

    merged = att_keyed.merge(
        eva_keyed[["Key", "Email Address", "Full Name", "Canonical Name"]],
        on="Key",
        how="outer",
        suffixes=("_Attendance", "_Evaluation"),
        indicator=True,
    )

    def pick_name(row: pd.Series) -> str:
        name_att = safe_text(row.get("Full Name_Attendance", ""))
        name_eva = safe_text(row.get("Full Name_Evaluation", ""))
        canon_att = safe_text(row.get("Canonical Name_Attendance", ""))
        canon_eva = safe_text(row.get("Canonical Name_Evaluation", ""))

        if name_att:
            return name_att
        if name_eva:
            return name_eva
        if canon_att:
            return title_case_name(canon_att)
        if canon_eva:
            return title_case_name(canon_eva)
        return ""

    def pick_email(row: pd.Series) -> str:
        email_att = safe_text(row.get("Email Address_Attendance", ""))
        email_eva = safe_text(row.get("Email Address_Evaluation", ""))
        return email_att if email_att else email_eva

    merged["Full Name"] = merged.apply(pick_name, axis=1)
    merged["Email Address"] = merged.apply(pick_email, axis=1)
    merged["Attendance Status"] = merged["_merge"].apply(
        lambda x: "Present in Attendance" if x in ("both", "left_only") else "Missing"
    )
    merged["Evaluation Status"] = merged["_merge"].apply(
        lambda x: "Submitted Evaluation" if x in ("both", "right_only") else "Missing"
    )
    merged["Eligibility Status"] = merged["_merge"].apply(
        lambda x: "Eligible" if x == "both" else "Not Eligible"
    )
    merged["Notes / Remarks"] = merged["_merge"].map(
        {
            "both": "",
            "left_only": "No evaluation record found",
            "right_only": "No attendance record found",
        }
    )

    final_master = merged[
        [
            "Full Name",
            "Email Address",
            "Attendance Status",
            "Evaluation Status",
            "Eligibility Status",
            "Notes / Remarks",
        ]
    ].sort_values(by=["Full Name"], ascending=[True], na_position="last")

    unmatched_att = merged[merged["_merge"] == "left_only"][
        ["Full Name", "Email Address", "Notes / Remarks"]
    ]
    unmatched_eva = merged[merged["_merge"] == "right_only"][
        ["Full Name", "Email Address", "Notes / Remarks"]
    ]
    return final_master.reset_index(drop=True), unmatched_att.reset_index(drop=True), unmatched_eva.reset_index(drop=True)


def sanitize_filename(name: str) -> str:
    cleaned = re.sub(r'[<>:"/\\|?*]+', "", name).strip()
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned or f"certificate-{uuid.uuid4().hex[:8]}"


def split_and_rename_certificates(
    pdf_bytes: bytes, master_df: pd.DataFrame, match_mode: str
) -> Tuple[bytes, pd.DataFrame]:
    reader = PdfReader(io.BytesIO(pdf_bytes))
    page_count = len(reader.pages)
    participants = master_df.copy()
    participants["Full Name"] = participants["Full Name"].fillna("").astype(str).str.strip()
    participants = participants[participants["Full Name"] != ""].reset_index(drop=True)

    if participants.empty:
        raise ValueError("No valid participant names found in the master list.")

    mapping_records = []
    files_bytes: Dict[str, bytes] = {}

    if match_mode == "sequence":
        take = min(page_count, len(participants))
        for i in range(take):
            person = participants.iloc[i]
            filename = sanitize_filename(person["Full Name"]) + ".pdf"
            writer = PdfWriter()
            writer.add_page(reader.pages[i])
            one_file = io.BytesIO()
            writer.write(one_file)
            files_bytes[filename] = one_file.getvalue()
            mapping_records.append(
                {
                    "Page #": i + 1,
                    "Participant": person["Full Name"],
                    "Filename": filename,
                    "Match Score": 100,
                    "Method": "Sequence",
                }
            )
    else:
        names = participants["Full Name"].fillna("").tolist()
        used_idx = set()
        for page_idx, page in enumerate(reader.pages, start=1):
            page_text = normalize_name_for_match(page.extract_text() or "")
            best_score = -1
            best_idx = None
            for idx, full_name in enumerate(names):
                if idx in used_idx:
                    continue
                score = fuzz.partial_ratio(normalize_name_for_match(full_name), page_text)
                if score > best_score:
                    best_score = score
                    best_idx = idx
            if best_idx is None:
                continue

            used_idx.add(best_idx)
            person = participants.iloc[best_idx]
            filename = sanitize_filename(person["Full Name"]) + ".pdf"
            writer = PdfWriter()
            writer.add_page(reader.pages[page_idx - 1])
            one_file = io.BytesIO()
            writer.write(one_file)
            files_bytes[filename] = one_file.getvalue()
            mapping_records.append(
                {
                    "Page #": page_idx,
                    "Participant": person["Full Name"],
                    "Filename": filename,
                    "Match Score": best_score,
                    "Method": "Name Match",
                }
            )

    zip_bytes = io.BytesIO()
    with zipfile.ZipFile(zip_bytes, "w", zipfile.ZIP_DEFLATED) as zf:
        for fname, content in files_bytes.items():
            zf.writestr(fname, content)
    return zip_bytes.getvalue(), pd.DataFrame(mapping_records)


def merge_pdf_files(pdf_contents: List[bytes]) -> Tuple[bytes, int]:
    if not pdf_contents or len(pdf_contents) < 2:
        raise ValueError("Please upload at least 2 PDF files to merge.")

    writer = PdfWriter()
    total_pages = 0

    for pdf_content in pdf_contents:
        reader = PdfReader(io.BytesIO(pdf_content))
        total_pages += len(reader.pages)
        for page in reader.pages:
            writer.add_page(page)

    output = io.BytesIO()
    writer.write(output)
    return output.getvalue(), total_pages


def build_pdf_upload_entries(uploaded_files: List) -> List[Dict[str, object]]:
    entries: List[Dict[str, object]] = []
    seen_keys: Dict[str, int] = {}

    for uploaded in uploaded_files:
        content = uploaded.getvalue()
        digest = hashlib.md5(content).hexdigest()[:12]
        base_key = f"{uploaded.name}::{len(content)}::{digest}"
        seen_keys[base_key] = seen_keys.get(base_key, 0) + 1
        file_id = f"{base_key}::{seen_keys[base_key]}"
        entries.append({"id": file_id, "name": uploaded.name, "bytes": content})
    return entries


def build_qr_payload(template: str, name: str, email: str, cert_file: str) -> str:
    return (
        template.replace("{name}", name)
        .replace("{email}", email)
        .replace("{filename}", cert_file)
    )


def create_qr_image(payload: str, box_size: int = 8):
    qr = qrcode.QRCode(version=None, box_size=box_size, border=2)
    qr.add_data(payload)
    qr.make(fit=True)
    return qr.make_image(fill_color="black", back_color="white").convert("RGB")


def embed_qr_into_pdf(
    original_pdf_bytes: bytes,
    qr_payload: str,
    text_label: str,
    position_mode: str,
    x_ratio: float,
    y_ratio: float,
    margin_ratio: float,
    qr_size_ratio: float,
) -> bytes:
    reader = PdfReader(io.BytesIO(original_pdf_bytes))
    if len(reader.pages) != 1:
        raise ValueError("Expected a one-page certificate PDF.")
    page = reader.pages[0]

    width = float(page.mediabox.width)
    height = float(page.mediabox.height)
    qr_size = min(width, height) * qr_size_ratio
    min_padding = 4.0
    margin = min(width, height) * margin_ratio

    if position_mode == "bottom_right":
        x = width - qr_size - margin
        y = margin
    elif position_mode == "bottom_left":
        x = margin
        y = margin
    elif position_mode == "top_right":
        x = width - qr_size - margin
        y = height - qr_size - margin
    elif position_mode == "top_left":
        x = margin
        y = height - qr_size - margin
    elif position_mode == "center":
        x = (width - qr_size) / 2
        y = (height - qr_size) / 2
    else:
        x = width * x_ratio
        y = height * y_ratio

    x = max(min_padding, min(x, width - qr_size - min_padding))
    y = max(min_padding, min(y, height - qr_size - min_padding))

    qr_img = create_qr_image(qr_payload)
    overlay_buffer = io.BytesIO()
    c = canvas.Canvas(overlay_buffer, pagesize=(width, height))
    c.drawImage(ImageReader(qr_img), x, y, qr_size, qr_size, preserveAspectRatio=True, mask="auto")
    if text_label.strip():
        safe_label = text_label[:100]
        label_font_size = 8
        label_gap = 2.0
        c.setFont("Helvetica", label_font_size)
        text_width = c.stringWidth(safe_label, "Helvetica", label_font_size)
        text_x = max(min_padding, min(x + (qr_size - text_width) / 2, width - text_width - min_padding))

        label_below_y = y - (label_font_size + label_gap)
        if label_below_y >= min_padding:
            c.drawString(text_x, label_below_y, safe_label)
        else:
            label_above_y = min(height - label_font_size - min_padding, y + qr_size + label_gap)
            c.drawString(text_x, label_above_y, safe_label)
    c.save()

    overlay_pdf = PdfReader(io.BytesIO(overlay_buffer.getvalue()))
    page.merge_page(overlay_pdf.pages[0])

    writer = PdfWriter()
    writer.add_page(page)
    output = io.BytesIO()
    writer.write(output)
    return output.getvalue()


def dataframe_to_csv_download(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8")


def main():
    st.markdown('<div class="main-title">Certificate Management System</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="sub-title">Automate participant validation, certificate preparation, and QR tagging.</div>',
        unsafe_allow_html=True,
    )

    if "master_df" not in st.session_state:
        st.session_state.master_df = pd.DataFrame()
    if "split_zip" not in st.session_state:
        st.session_state.split_zip = None
    if "split_mapping" not in st.session_state:
        st.session_state.split_mapping = pd.DataFrame()
    if "renamed_files" not in st.session_state:
        st.session_state.renamed_files = {}
    if "merged_pdf_bytes" not in st.session_state:
        st.session_state.merged_pdf_bytes = None
    if "merged_pdf_name" not in st.session_state:
        st.session_state.merged_pdf_name = ""
    if "merge_order_ids" not in st.session_state:
        st.session_state.merge_order_ids = []
    if "master_dupes" not in st.session_state:
        st.session_state.master_dupes = pd.DataFrame()

    tab1, tab2, tab3, tab4 = st.tabs(
        [
            "1) Participant Cross-Check",
            "2) Merge Canva PDFs",
            "3) Split + Rename Certificates",
            "4) QR Tag Certificates",
        ]
    )

    with tab1:
        st.subheader("Upload Attendance and Evaluation CSV Files")
        col_a, col_b = st.columns(2)
        with col_a:
            attendance_file = st.file_uploader("Attendance CSV", type=["csv"], key="attendance_csv")
        with col_b:
            evaluation_file = st.file_uploader("Evaluation CSV", type=["csv"], key="evaluation_csv")

        uppercase_names = st.toggle("Output names in UPPERCASE", value=False)

        if st.button("Process and Generate Master List", type="primary"):
            if not attendance_file or not evaluation_file:
                st.error("Please upload both attendance and evaluation CSV files.")
            else:
                try:
                    att_df_raw = pd.read_csv(attendance_file)
                    eva_df_raw = pd.read_csv(evaluation_file)

                    att_parsed, att_detected = parse_uploaded_csv(att_df_raw, "Attendance", uppercase_names)
                    eva_parsed, eva_detected = parse_uploaded_csv(eva_df_raw, "Evaluation", uppercase_names)

                    att_deduped, att_dupes = deduplicate_participants(att_parsed)
                    eva_deduped, eva_dupes = deduplicate_participants(eva_parsed)
                    master_df, unmatched_att, unmatched_eva = build_master_list(att_deduped, eva_deduped)

                    st.session_state.master_df = master_df
                    st.session_state.unmatched_att = unmatched_att
                    st.session_state.unmatched_eva = unmatched_eva
                    st.session_state.att_dupes = att_dupes
                    st.session_state.eva_dupes = eva_dupes
                    st.session_state.master_dupes = analyze_duplicates(master_df)

                    st.success("Master list generated successfully.")
                    st.write("Detected Attendance Columns:", att_detected)
                    st.write("Detected Evaluation Columns:", eva_detected)
                except Exception as exc:
                    st.exception(exc)

        if not st.session_state.master_df.empty:
            st.markdown("### Final Participant Master List")
            st.dataframe(st.session_state.master_df, use_container_width=True)
            st.download_button(
                "Download Master List CSV",
                data=dataframe_to_csv_download(st.session_state.master_df),
                file_name="final_participant_master_list.csv",
                mime="text/csv",
            )
            st.markdown("### Final Master List Duplicate Audit")
            if st.session_state.master_dupes.empty:
                st.info("No duplicate names or duplicate emails detected in the final master list.")
            else:
                st.dataframe(st.session_state.master_dupes, use_container_width=True)

            st.markdown("### Possible Duplicates / Inconsistencies")
            col1, col2 = st.columns(2)
            with col1:
                st.write("Attendance Duplicates")
                st.dataframe(st.session_state.att_dupes, use_container_width=True)
            with col2:
                st.write("Evaluation Duplicates")
                st.dataframe(st.session_state.eva_dupes, use_container_width=True)

            st.markdown("### Unmatched Entries")
            col3, col4 = st.columns(2)
            with col3:
                st.write("In Attendance but missing Evaluation")
                st.dataframe(st.session_state.unmatched_att, use_container_width=True)
            with col4:
                st.write("In Evaluation but missing Attendance")
                st.dataframe(st.session_state.unmatched_eva, use_container_width=True)

    with tab2:
        st.subheader("Merge Canva Certificate PDFs")
        merge_files = st.file_uploader(
            "Upload Canva PDF files in correct order",
            type=["pdf"],
            accept_multiple_files=True,
            key="merge_pdf_files",
        )
        st.caption("Canva may split exports every 80 pages. Merge all parts here before Step 3.")
        st.caption("You can re-arrange files incrementally using Up/Down before merging.")

        entries_map: Dict[str, Dict[str, object]] = {}
        if merge_files:
            incoming_entries = build_pdf_upload_entries(merge_files)
            entries_map = {entry["id"]: entry for entry in incoming_entries}
            incoming_ids = list(entries_map.keys())
            saved_ids = st.session_state.merge_order_ids

            if not saved_ids:
                st.session_state.merge_order_ids = incoming_ids
            else:
                kept_ids = [file_id for file_id in saved_ids if file_id in entries_map]
                new_ids = [file_id for file_id in incoming_ids if file_id not in kept_ids]
                st.session_state.merge_order_ids = kept_ids + new_ids

            st.markdown("### Merge Order")
            ordered_ids = st.session_state.merge_order_ids
            for idx, file_id in enumerate(ordered_ids):
                file_entry = entries_map[file_id]
                col_idx, col_name, col_up, col_down = st.columns([0.7, 7, 1, 1])
                with col_idx:
                    st.write(f"{idx + 1}.")
                with col_name:
                    st.write(file_entry["name"])
                with col_up:
                    if st.button("Up", key=f"merge_up_{file_id}", disabled=idx == 0):
                        ordered_ids[idx - 1], ordered_ids[idx] = ordered_ids[idx], ordered_ids[idx - 1]
                        st.session_state.merge_order_ids = ordered_ids
                        st.rerun()
                with col_down:
                    if st.button("Down", key=f"merge_down_{file_id}", disabled=idx == len(ordered_ids) - 1):
                        ordered_ids[idx + 1], ordered_ids[idx] = ordered_ids[idx], ordered_ids[idx + 1]
                        st.session_state.merge_order_ids = ordered_ids
                        st.rerun()

            if st.button("Reset to Upload Order", key="reset_merge_order"):
                st.session_state.merge_order_ids = incoming_ids
                st.rerun()

        if st.button("Merge PDF Files", type="primary"):
            if not merge_files or len(merge_files) < 2:
                st.error("Please upload at least 2 PDF files to merge.")
            else:
                try:
                    ordered_pdf_contents = [
                        entries_map[file_id]["bytes"] for file_id in st.session_state.merge_order_ids
                    ]
                    merged_blob, total_pages = merge_pdf_files(ordered_pdf_contents)
                    st.session_state.merged_pdf_bytes = merged_blob
                    st.session_state.merged_pdf_name = "merged_canva_certificates.pdf"
                    st.success(f"Merged {len(ordered_pdf_contents)} files into 1 PDF ({total_pages} pages).")
                except Exception as exc:
                    st.exception(exc)

        if st.session_state.merged_pdf_bytes is not None:
            st.download_button(
                "Download Merged PDF",
                data=st.session_state.merged_pdf_bytes,
                file_name=st.session_state.merged_pdf_name,
                mime="application/pdf",
            )

    with tab3:
        st.subheader("Upload Bulk Canva Certificate PDF")
        uploaded_pdf = st.file_uploader("Bulk Certificate PDF", type=["pdf"], key="bulk_pdf")
        has_merged_pdf = st.session_state.merged_pdf_bytes is not None
        pdf_bytes_for_split = st.session_state.merged_pdf_bytes if has_merged_pdf else None

        if has_merged_pdf:
            st.info("Using merged PDF from Step 2. Upload a file below only if you want to override it.")
        if uploaded_pdf is not None:
            pdf_bytes_for_split = uploaded_pdf.read()

        match_mode = st.radio(
            "Matching Logic",
            options=["sequence", "name"],
            format_func=lambda x: "Row Sequence" if x == "sequence" else "Name Matching (fuzzy)",
            horizontal=True,
        )
        st.caption("Use sequence if Canva page order exactly matches your final list.")

        if st.button("Split and Rename Certificates", type="primary"):
            if pdf_bytes_for_split is None:
                st.error("Upload a bulk certificate PDF in Step 3 or merge files in Step 2 first.")
            elif st.session_state.master_df.empty:
                st.error("Generate the final participant master list in Step 1 first.")
            else:
                try:
                    zip_blob, mapping_df = split_and_rename_certificates(
                        pdf_bytes_for_split, st.session_state.master_df, match_mode
                    )
                    st.session_state.split_zip = zip_blob
                    st.session_state.split_mapping = mapping_df
                    st.success("Certificates split and renamed successfully.")
                except Exception as exc:
                    st.exception(exc)

        if st.session_state.split_zip is not None:
            st.dataframe(st.session_state.split_mapping, use_container_width=True)
            st.download_button(
                "Download Split + Renamed Certificates (.zip)",
                data=st.session_state.split_zip,
                file_name="split_renamed_certificates.zip",
                mime="application/zip",
            )

    with tab4:
        st.subheader("QR Code Tagging and Final Release Package")
        cert_zip = st.file_uploader(
            "Upload split/renamed certificate ZIP (from Step 3)",
            type=["zip"],
            key="renamed_zip",
        )
        verify_template = st.text_input(
            "Verification Link/Text Template",
            value="https://example.com/verify?name={name}&email={email}",
            help="Use placeholders: {name}, {email}, {filename}",
        )
        qr_label = st.text_input("Short text near QR (optional)", value="Verify this certificate")

        c1, c2, c3 = st.columns(3)
        with c1:
            position_mode = st.selectbox(
                "QR Alignment",
                options=[
                    "bottom_right",
                    "bottom_left",
                    "top_right",
                    "top_left",
                    "center",
                    "custom",
                ],
                format_func=lambda x: {
                    "bottom_right": "Bottom Right",
                    "bottom_left": "Bottom Left",
                    "top_right": "Top Right",
                    "top_left": "Top Left",
                    "center": "Center",
                    "custom": "Custom (manual)",
                }[x],
                index=0,
            )
        with c2:
            margin_ratio = st.slider(
                "Edge Margin",
                min_value=0.0,
                max_value=0.20,
                value=0.03,
                step=0.005,
                help="Used for aligned positions (not custom mode).",
            )
        with c3:
            qr_size_ratio = st.slider("QR Size Ratio", min_value=0.05, max_value=0.30, value=0.12, step=0.01)

        x_ratio = 0.80
        y_ratio = 0.08
        if position_mode == "custom":
            cc1, cc2 = st.columns(2)
            with cc1:
                x_ratio = st.slider("Custom X Position", min_value=0.0, max_value=0.95, value=0.80, step=0.01)
            with cc2:
                y_ratio = st.slider("Custom Y Position", min_value=0.0, max_value=0.95, value=0.08, step=0.01)

        if st.button("Apply QR Tag to Certificates", type="primary"):
            if cert_zip is None:
                st.error("Upload a ZIP file of split/renamed certificates.")
            else:
                try:
                    name_email_map = {}
                    if not st.session_state.master_df.empty:
                        for _, row in st.session_state.master_df.iterrows():
                            name_email_map[normalize_name_for_match(row["Full Name"])] = row["Email Address"]

                    input_zip = zipfile.ZipFile(io.BytesIO(cert_zip.read()), "r")
                    output_blob = io.BytesIO()
                    report_rows = []

                    with zipfile.ZipFile(output_blob, "w", zipfile.ZIP_DEFLATED) as out_zip:
                        for fname in input_zip.namelist():
                            if not fname.lower().endswith(".pdf"):
                                continue
                            content = input_zip.read(fname)
                            participant_name = fname.rsplit(".", 1)[0]
                            email = name_email_map.get(normalize_name_for_match(participant_name), "")
                            payload = build_qr_payload(verify_template, participant_name, email, fname)
                            tagged_pdf = embed_qr_into_pdf(
                                content,
                                payload,
                                qr_label,
                                position_mode,
                                x_ratio,
                                y_ratio,
                                margin_ratio,
                                qr_size_ratio,
                            )
                            out_zip.writestr(fname, tagged_pdf)
                            report_rows.append(
                                {
                                    "Filename": fname,
                                    "Participant": participant_name,
                                    "Email Address": email,
                                    "QR Payload": payload,
                                }
                            )

                    st.session_state.final_zip = output_blob.getvalue()
                    st.session_state.final_report = pd.DataFrame(report_rows)
                    st.success("QR tags applied to all certificates.")
                except Exception as exc:
                    st.exception(exc)

        if "final_zip" in st.session_state and st.session_state.final_zip is not None:
            st.dataframe(st.session_state.final_report, use_container_width=True)
            st.download_button(
                "Download Final QR-Tagged Certificates (.zip)",
                data=st.session_state.final_zip,
                file_name="final_qr_tagged_certificates.zip",
                mime="application/zip",
            )


if __name__ == "__main__":
    main()
