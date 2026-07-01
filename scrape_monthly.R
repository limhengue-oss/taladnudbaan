# =============================================================================
# scrape_monthly.R
# รันวันที่ 1 ของทุกเดือน 01:00 ICT
# ไฟล์ถูกโหลด/อัพโหลดโดย rclone ใน workflow แล้ว
# script นี้แค่ rebuild taladnudbaan_properties.csv จากไฟล์ที่ดาวน์โหลดมา
# =============================================================================

# ---- CONFIG -----------------------------------------------------------------
CONFIG <- list(
  work_dir        = "C:/Users/wasinr/OneDrive - Bank of Thailand/My Github/taladnudbaan",
  properties_file = "taladnudbaan_properties.csv",
  status_file     = "update_status.txt"
)

# ---- SETUP ------------------------------------------------------------------
if (Sys.getenv("GITHUB_ACTIONS") != "true") {
  if (!dir.exists(CONFIG$work_dir)) stop("ไม่พบ work_dir: ", CONFIG$work_dir)
  setwd(CONFIG$work_dir)
}

pkgs <- c("dplyr", "readr", "stringr")
missing <- pkgs[!vapply(pkgs, requireNamespace, logical(1), quietly = TRUE)]
if (length(missing) > 0) stop("ติดตั้ง packages ก่อน: install.packages(c(",
  paste(sprintf('"%s"', missing), collapse = ", "), "))")
library(dplyr); library(readr); library(stringr)

# ---- HELPERS ----------------------------------------------------------------
prev_month_stamp <- function() {
  today      <- as.Date(format(Sys.time(), tz = "Asia/Bangkok"))
  last_month <- as.Date(format(today, "%Y-%m-01")) - 1
  format(last_month, "%Y%m")
}

old_file_suffix <- function(file_path) {
  mtime <- as.Date(file.info(file_path)$mtime)
  format(mtime, "%b%Y")
}

# ---- MAIN -------------------------------------------------------------------
message("=== MONTHLY REBUILD (", format(Sys.Date(), "%Y-%m-%d"), ") ===")

# 1) เช็ค unupdated ค้างอยู่ไหม
message("\n--- STEP 1: เช็คสถานะ ---")
if (!file.exists(CONFIG$status_file)) {
  stop("ไม่พบ ", CONFIG$status_file)
}
status <- readLines(CONFIG$status_file, warn = FALSE)[1]
message("  status: ", status)
if (str_starts(status, "unupdated:")) {
  n_left <- str_extract(status, "[0-9]+$")
  stop("มีรายการค้างอยู่ ", n_left, " รายการ -> รอให้ scrape_daily ทำเสร็จก่อน")
}
message("  ✅ ไม่มีค้าง -> ดำเนินการ rebuild")

# 2) โหลด properties เดิม
message("\n--- STEP 2: โหลด properties เดิม ---")
if (!file.exists(CONFIG$properties_file)) stop("ไม่พบ ", CONFIG$properties_file)
props <- read_csv(CONFIG$properties_file, col_types = cols(.default = "c"))
message("  properties เดิม: ", nrow(props), " แถว")

# 3) โหลด changelog + detail_update ของเดือนที่แล้ว (ถูก rclone โหลดมาแล้ว)
message("\n--- STEP 3: โหลดข้อมูลเดือนที่แล้ว ---")
pm <- prev_month_stamp()

changelog_files    <- list.files(".", pattern = paste0("^changelog_",     pm, ".*\\.csv$"))
detail_update_files <- list.files(".", pattern = paste0("^detail_update_", pm, ".*\\.csv$"))

if (length(changelog_files) == 0) stop("ไม่พบ changelog ของเดือน ", pm)

changelog <- bind_rows(lapply(changelog_files, read_csv, col_types = cols(.default = "c")))
detail_update <- if (length(detail_update_files) > 0)
  bind_rows(lapply(detail_update_files, read_csv, col_types = cols(.default = "c")))
else props[0, ]

message("  changelog: ", nrow(changelog), " แถว")
message("  detail_update: ", nrow(detail_update), " แถว")

# 4) แยก change_type
new_urls     <- changelog |> filter(change_type == "new")     |> pull(url) |> unique()
updated_urls <- changelog |> filter(change_type == "updated") |> pull(url) |> unique()
removed_urls <- changelog |> filter(change_type == "removed") |> pull(url) |> unique()
message("\n--- STEP 4: DIFF ---")
message("  new=", length(new_urls), " updated=", length(updated_urls), " removed=", length(removed_urls))

# 5) เตรียม detail ใหม่ (url ซ้ำ -> เอาล่าสุด)
new_detail <- detail_update |>
  filter(url %in% c(new_urls, updated_urls)) |>
  arrange(desc(scraped_at)) |>
  distinct(url, .keep_all = TRUE)

# 6) upsert + ลบ removed
message("\n--- STEP 5: REBUILD ---")
props_new <- bind_rows(new_detail, props) |>
  distinct(url, .keep_all = TRUE) |>
  filter(!url %in% removed_urls)
message("  properties ใหม่: ", nrow(props_new), " แถว")

# 7) rename ไฟล์เดิม
suffix   <- old_file_suffix(CONFIG$properties_file)
old_name <- str_replace(CONFIG$properties_file, "\\.csv$", paste0("_", suffix, ".csv"))
file.rename(CONFIG$properties_file, old_name)
message("\n--- STEP 6: RENAME ---")
message("  ", CONFIG$properties_file, " -> ", old_name)

# 8) save ใหม่
write_excel_csv(props_new, CONFIG$properties_file)
message("\n--- STEP 7: SAVE ---")
message("  ", CONFIG$properties_file, " (", nrow(props_new), " แถว)")

message("\n=== MONTHLY REBUILD เสร็จ ===")
