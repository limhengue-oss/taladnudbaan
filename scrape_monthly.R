# =============================================================================
# scrape_monthly.R
# รันวันที่ 1 ของทุกเดือน 01:00 ICT (GitHub Actions) หรือรันเองบนเครื่อง
# 1) เช็คว่ามี unupdated ค้างอยู่ไหม -> ถ้ามีให้หยุด
# 2) รวม changelog + detail_update ทั้งเดือนจาก Drive
# 3) upsert new/updated เข้า taladnudbaan_properties.csv
# 4) ลบ removed ออก
# 5) rename ไฟล์เก่า -> taladnudbaan_properties_MonYYYY.csv
# 6) save taladnudbaan_properties.csv ใหม่
# =============================================================================

# ---- CONFIG -----------------------------------------------------------------
CONFIG <- list(
  work_dir    = "C:/Users/wasinr/OneDrive - Bank of Thailand/My Github/taladnudbaan",

  properties_file = "taladnudbaan_properties.csv",
  status_file     = "update_status.txt",   # เขียนโดย scrape_daily.R
  gdrive_folder   = "1osS1NHpNkiwaRXywcpEOcSueG30Z-jyv"
)

# ---- SETUP ------------------------------------------------------------------
if (Sys.getenv("GITHUB_ACTIONS") != "true") {
  if (!dir.exists(CONFIG$work_dir)) stop("ไม่พบ work_dir: ", CONFIG$work_dir)
  setwd(CONFIG$work_dir)
}

pkgs <- c("dplyr", "readr", "stringr", "googledrive")
missing <- pkgs[!vapply(pkgs, requireNamespace, logical(1), quietly = TRUE)]
if (length(missing) > 0) stop("ติดตั้ง packages ก่อน: install.packages(c(",
  paste(sprintf('"%s"', missing), collapse = ", "), "))")
library(dplyr); library(readr); library(stringr); library(googledrive)

# ---- DRIVE AUTH -------------------------------------------------------------
drive_init <- function() {
  token_b64 <- Sys.getenv("GDRIVE_TOKEN")
  if (!nzchar(token_b64)) stop("ไม่พบ env GDRIVE_TOKEN")
  decoded <- base64enc::base64decode(token_b64)
  writeBin(decoded, "gdrive_token.rds")
  drive_auth(token = readRDS("gdrive_token.rds"))
  message("Drive auth OK")
}

# โหลดทุกไฟล์ที่ขึ้นต้นด้วย prefix ใน Drive -> bind รวมกัน
drive_read_prefix <- function(prefix) {
  hits <- drive_ls(as_id(CONFIG$gdrive_folder), pattern = paste0("^", prefix))
  if (nrow(hits) == 0) return(NULL)
  hits <- hits[order(hits$name), ]
  tmp <- tempfile(fileext = ".csv")
  dfs <- lapply(seq_len(nrow(hits)), function(i) {
    drive_download(hits[i, ], path = tmp, overwrite = TRUE)
    read_csv(tmp, col_types = cols(.default = "c"))
  })
  message("  โหลด ", nrow(hits), " ไฟล์ prefix '", prefix, "'")
  bind_rows(dfs)
}

# โหลดไฟล์เดียวจาก Drive ตามชื่อ
drive_read_file <- function(name, local_path) {
  hits <- drive_ls(as_id(CONFIG$gdrive_folder), pattern = paste0("^", name, "$"))
  if (nrow(hits) == 0) stop("ไม่พบไฟล์บน Drive: ", name)
  drive_download(hits[1, ], path = local_path, overwrite = TRUE)
  message("  download <- Drive: ", name)
}

# upload file ใหม่ (ไม่ทับ)
drive_upload_new <- function(local) {
  drive_upload(local, path = as_id(CONFIG$gdrive_folder),
               name = basename(local), overwrite = FALSE)
  message("  upload (new) -> Drive: ", basename(local))
}

# ---- HELPERS ----------------------------------------------------------------
# เดือนที่แล้ว (ICT) สำหรับ prefix ค้นหาไฟล์ของเดือนที่แล้ว
prev_month_stamp <- function() {
  today <- as.Date(format(Sys.time(), tz = "Asia/Bangkok"))
  first_of_this <- as.Date(format(today, "%Y-%m-01"))
  last_month <- first_of_this - 1
  format(last_month, "%Y%m")   # เช่น "202607"
}

# format ชื่อไฟล์เก่า: taladnudbaan_properties_Jul2026.csv
old_file_suffix <- function(file_path) {
  info <- file.info(file_path)
  mtime <- as.Date(info$mtime)
  format(mtime, "%b%Y")   # เช่น "Jun2026"
}

# ---- MAIN -------------------------------------------------------------------
message("=== MONTHLY REBUILD (", format(Sys.Date(), "%Y-%m-%d"), ") ===")

# init Drive (ถ้ารันบน GitHub Actions)
if (Sys.getenv("GITHUB_ACTIONS") == "true") drive_init()

# 1) เช็ค unupdated ค้างอยู่ไหม
message("\n--- STEP 1: เช็คสถานะ unupdated ---")
if (Sys.getenv("GITHUB_ACTIONS") == "true") {
  drive_read_file("update_status.txt", "update_status.txt")
}
if (!file.exists(CONFIG$status_file)) {
  stop("ไม่พบ ", CONFIG$status_file, " -> ต้องรัน scrape_daily.R ก่อนอย่างน้อย 1 ครั้ง")
}
status <- readLines(CONFIG$status_file, warn = FALSE)[1]
message("  status: ", status)
if (str_starts(status, "unupdated:")) {
  n_left <- str_extract(status, "[0-9]+$")
  stop("มีรายการค้างอยู่ ", n_left, " รายการ -> รอให้ scrape_daily ทำเสร็จก่อนแล้วค่อย rebuild")
}
message("  ✅ ไม่มีค้าง -> ดำเนินการ rebuild ได้")

# 2) โหลด properties เดิม
message("\n--- STEP 2: โหลด properties เดิม ---")
if (!file.exists(CONFIG$properties_file)) stop("ไม่พบ ", CONFIG$properties_file)
props <- read_csv(CONFIG$properties_file, col_types = cols(.default = "c"))
message("  properties เดิม: ", nrow(props), " แถว")

# 3) โหลด changelog + detail_update ของเดือนที่แล้วจาก Drive
message("\n--- STEP 3: โหลดข้อมูลเดือนที่แล้วจาก Drive ---")
pm <- prev_month_stamp()
changelog    <- drive_read_prefix(paste0("changelog_",     pm))
detail_update <- drive_read_prefix(paste0("detail_update_", pm))

if (is.null(changelog)) stop("ไม่พบ changelog ของเดือน ", pm, " บน Drive")
if (is.null(detail_update)) {
  message("  ไม่มี detail_update ของเดือน ", pm, " -> ไม่มีรายการใหม่/อัพเดท")
  detail_update <- props[0, ]
}

message("  changelog: ", nrow(changelog), " แถว")
message("  detail_update: ", nrow(detail_update), " แถว")

# 4) แยก change_type
new_urls     <- changelog |> filter(change_type == "new")     |> pull(url) |> unique()
updated_urls <- changelog |> filter(change_type == "updated") |> pull(url) |> unique()
removed_urls <- changelog |> filter(change_type == "removed") |> pull(url) |> unique()
message("\n--- STEP 4: DIFF SUMMARY ---")
message("  new: ", length(new_urls), " | updated: ", length(updated_urls),
        " | removed: ", length(removed_urls))

# 5) เตรียม detail ใหม่ (url ที่มีหลายครั้งใน detail_update -> เอาล่าสุด)
new_detail <- detail_update |>
  filter(url %in% c(new_urls, updated_urls)) |>
  arrange(desc(scraped_at)) |>
  distinct(url, .keep_all = TRUE)

# 6) upsert + ลบ removed
message("\n--- STEP 5: REBUILD ---")
props_new <- bind_rows(new_detail, props) |>
  distinct(url, .keep_all = TRUE) |>   # new_detail ชนะ props เดิม (อยู่บนสุด)
  filter(!url %in% removed_urls)
message("  properties ใหม่: ", nrow(props_new), " แถว",
        " (เพิ่ม ", length(new_urls), " ลบ ", length(removed_urls),
        " อัพเดท ", length(updated_urls), ")")

# 7) rename ไฟล์เดิม -> taladnudbaan_properties_MonYYYY.csv
suffix   <- old_file_suffix(CONFIG$properties_file)
old_name <- str_replace(CONFIG$properties_file, "\\.csv$", paste0("_", suffix, ".csv"))
file.rename(CONFIG$properties_file, old_name)
message("\n--- STEP 6: RENAME ---")
message("  ", CONFIG$properties_file, " -> ", old_name)

# 8) save properties ใหม่
write_excel_csv(props_new, CONFIG$properties_file)
message("\n--- STEP 7: SAVE ---")
message("  ", CONFIG$properties_file, " (", nrow(props_new), " แถว)")

# 9) upload ขึ้น Drive (ทั้ง properties ใหม่และ archived เก่า)
if (Sys.getenv("GITHUB_ACTIONS") == "true") {
  drive_upload_new(old_name)
  drive_upload_new(CONFIG$properties_file)
}

message("\n=== MONTHLY REBUILD เสร็จ ===")
