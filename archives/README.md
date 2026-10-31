# archives

ไฟล์ที่เลิกใช้แล้ว ย้ายมาเก็บไว้ก่อนลบจริง (ไม่มีโค้ดหรือ workflow ไหนเรียกใช้)

| ไฟล์ | เดิมอยู่ที่ | เหตุผลที่เลิกใช้ |
|---|---|---|
| `run_monthly.ps1` | `local/` | เรียก `scrape_monthly.R` ที่ไม่มีแล้ว (BUGS.md #5) |
| `scrape_monthly_full_test.yml` | `.github/workflows/` | ไม่ได้ตั้ง PHASE จึงรันไม่จบใน 30 นาที (BUGS.md #6) อยู่นอก `.github/workflows/` จึงไม่ถูกรัน |
| `load_secret.R` | root | ไม่มีไฟล์ไหนเรียกใช้ (BUGS.md #7) |
| `taladnudbaan_urls.RData` | root | baseline สำเนาเก่า 2026-09-13 ทุก workflow ดาวน์โหลดตัวจริงจาก Drive ทับเสมอ |
