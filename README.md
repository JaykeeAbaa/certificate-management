# Multi-Purpose Certificate Management System

End-to-end workflow for certificate issuance:
- Cross-check attendance + evaluation CSV files
- Generate final participant master list
- Split bulk Canva PDF certificates and auto-rename files
- Add QR code verification tags to each certificate
- Download release-ready certificate package

## Recommended System Design / Architecture

### 1) Presentation Layer (Web UI)
- Framework: Streamlit
- Guided, step-based workflow using 3 tabs:
  1. Participant Cross-Check
  2. Split + Rename Certificates
  3. QR Tag Certificates

### 2) Processing Layer (Business Logic)
- CSV parsing and column auto-detection
- Name normalization and certificate-ready formatting
- Cross-check, duplicate detection, unmatched detection
- PDF splitting and renaming
- QR generation and PDF embedding

### 3) Output Layer
- Final participant master list (CSV)
- Split/renamed certificates (ZIP of PDFs)
- Final QR-tagged certificates (ZIP of PDFs)
- Processing report tables for audit and validation

## Suggested Tech Stack

- **Frontend + app server**: Streamlit
- **Data processing**: Pandas
- **PDF processing**: pypdf
- **QR generation**: qrcode + Pillow
- **PDF overlay rendering**: reportlab
- **Fuzzy name matching**: rapidfuzz

## Step-by-Step Workflow

1. Upload attendance CSV and evaluation CSV.
2. System auto-detects relevant columns (email, name parts, full name, etc.).
3. Records are normalized, deduplicated, and cross-checked.
4. Final master list is generated with eligibility status.
5. User reviews duplicates/unmatched entries and downloads master CSV.
6. Upload bulk Canva PDF export.
7. Choose matching mode:
   - sequence (row/page order)
   - name matching (fuzzy text matching from page content)
8. System splits PDF into one-page certificates and renames files.
9. Upload renamed ZIP for QR tagging.
10. Provide verification template text/link.
11. System generates per-file QR and embeds it at consistent coordinates.
12. Download final QR-tagged ZIP for release.

## Matching and Validation Logic

### Column Auto-Detection
- Header names are normalized and matched using:
  - exact semantic aliases
  - partial/contains fallback
- Handles variable CSV column names across events.

### Participant Identity Resolution
- Primary key: normalized email.
- Secondary key: normalized full name when email is missing.
- Name consolidation:
  - Uses First/Middle/Last if available
  - Falls back to Full Name splitting otherwise
- Output supports title case or uppercase names.

### Duplicate Detection
- Dedup key = email if available, otherwise canonical full name.
- Duplicate groups are surfaced for review.
- First occurrence retained in automated dedup output.

### Eligibility Rules
- `Eligible` if participant exists in both attendance and evaluation datasets.
- `Not Eligible` otherwise.
- Unmatched entries from each source are reported.

## File Processing Approach

### CSV Parsing
- Read using Pandas.
- Normalize text values and strip formatting inconsistencies.

### PDF Splitting
- Read bulk PDF with `PdfReader`.
- Extract each page to a new single-page PDF with `PdfWriter`.

### Auto-Renaming
- Output filename format: `<Participant Full Name>.pdf`.
- Illegal file characters are sanitized.
- Supports sequence or fuzzy name matching.

### QR Generation
- Build payload from user template:
  - placeholders: `{name}`, `{email}`, `{filename}`
- Create QR image using `qrcode`.

### QR Embedding into PDF
- Build an overlay PDF (reportlab) containing QR + optional text.
- Merge overlay into each one-page certificate using pypdf.
- Preserve original filename for release consistency.

## Suggested UI Flow

- Keep steps linear and visible:
  - Step 1 complete before Step 2
  - Step 2 complete before Step 3
- Show:
  - Detected columns
  - Duplicate and unmatched tables
  - Split/rename mapping
  - QR tagging report
- Provide download buttons per step to minimize confusion.

## Error Handling Scenarios

- Missing file uploads
- Missing required columns (email/name fallbacks fail)
- Empty eligible list
- PDF page count mismatch with participant list
- Name matching confidence too low (review mapping table)
- Non-PDF files inside ZIP
- Corrupt/invalid PDF file

## Deployment Recommendations

### Local (fastest)
```bash
pip install -r requirements.txt
streamlit run app.py
```

### Cloud options
- Streamlit Community Cloud (small-medium workloads)
- Railway / Render / Fly.io (containerized deployment)
- Internal hosting on a secured office server if handling sensitive participant data

### Production hardening
- Add role-based access / login
- Add encrypted storage for uploaded files
- Add audit logs (who processed what, when)
- Add configurable eligibility rules
- Add confidence thresholds for fuzzy matching and manual correction UI

## Files

- `app.py` - complete web application
- `requirements.txt` - Python dependencies
- `README.md` - architecture and usage documentation
