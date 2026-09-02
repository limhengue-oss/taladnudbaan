# =============================================================================
# scrape_monthly_full.R
# รัน manual จาก local เท่านั้น (ไม่ auto, ไม่ผูกกับ GitHub Actions)
#
# วัตถุประสงค์: scrape detail ของ "ทุก url ที่ list บนเว็บตอนนี้" ใหม่หมด
# ไม่พึ่ง diff/baseline เหมือน scrape_daily.R -> ใช้เป็น audit / cross-check
# ว่าข้อมูลจาก incremental pipeline (scrape_daily.R) ตกหล่นหรือไม่ตรงจริงหรือเปล่า
#
# ไม่แตะ/ไม่เขียนทับ taladnudbaan_urls.RData หรือ taladnudbaan_properties.csv
# ของ pipeline หลัก -> output แยกไฟล์ชัดเจน กัน overwrite ของจริงพลาด
#
# งานนี้มี url เป็นแสน -> รันครั้งเดียวไม่จบแน่นอน มี resume ในตัว
# (pending queue เก็บลง taladnudbaan_full_pending.RData, สั่งรันสคริปต์ซ้ำได้เรื่อยๆ
#  จนกว่าจะครบ ค่อยเขียน taladnudbaan_properties_full_<timestamp>.csv ก้อนสุดท้าย)
#
# usage:
#   Rscript scrape_monthly_full.R                       # รันจนจบ ไม่มี time limit
#   TIME_LIMIT_MIN=180 Rscript scrape_monthly_full.R     # ถ้าอยากจำกัดเวลาต่อรอบ (นาที) ก็ยังทำได้
# =============================================================================

CONFIG <- list(
  base_url    = "https://www.taladnudbaan.com/properties?sellers_only_out=on&order=price%20desc&view=list&page_length=60&",
  sleep_sec   = 0.1,
  max_retries = 4L,
  backoff_sec = 20,
  max_pages   = 9000L,
  chunk_size  = 500L,

  pending_rdata = "taladnudbaan_full_pending.RData",   # -> full_urls, full_accum (list ของ tibble ที่ scrape ไปแล้ว)

  user_agent = "Mozilla/5.0 (research data collection; contact: limhengue@gmail.com)",

  field_labels = c(
    deed_type    = "ประเภทเอกสารสิทธิ์",
    land_size    = "ขนาดที่ดิน",
    usable_area  = "พื้นที่ใช้สอย",
    bedrooms     = "ห้องนอน",
    bathrooms    = "ห้องน้ำ",
    subdistrict  = "แขวง / ตำบล",
    contact_name = "ชื่อผู้ติดต่อ",
    agency_name  = "ชื่อหน่วยงานที่ประกาศทรัพย์",
    contact_info = "ข้อมูลการติดต่อ"
  )
)

TIME_LIMIT_MIN <- as.numeric(Sys.getenv("TIME_LIMIT_MIN", "Inf"))

WORKERS <- as.integer(Sys.getenv("WORKERS", "10"))  # จำนวน request ทำพร้อมกัน -> เร่งความเร็วรวมได้ ~WORKERS เท่า
                                                     # ค่านี้สูง เสี่ยงโดน rate-limit/block มากกว่าค่าต่ำ -> เฝ้าดู log ช่วง 30 นาทีแรก
                                                     # ถ้าเจอ error/429 ถี่ ให้ Ctrl+C แล้วรันใหม่ด้วย WORKERS ต่ำกว่านี้ (มี checkpoint resume ให้)

# ---- SETUP ------------------------------------------------------------------
pkgs <- c("rvest", "dplyr", "stringr", "purrr", "readr", "httr", "jsonlite", "future", "furrr")
missing <- pkgs[!vapply(pkgs, requireNamespace, logical(1), quietly = TRUE)]
if (length(missing) > 0) stop("ติดตั้ง packages ก่อน: install.packages(c(",
                              paste(sprintf('"%s"', missing), collapse=", "),"))")
library(rvest); library(dplyr); library(stringr); library(purrr); library(readr)
library(future); library(furrr)

# งานนี้เป็น I/O-bound (รอ network response เป็นหลัก ไม่ใช่ CPU-heavy)
# future/parallelly มี hard limit กันตั้ง worker เกิน 300% ของ CPU cores จริงโดย default
# (เผื่อ CPU-bound task) ไม่เหมาะกับงาน scrape ที่ CPU ว่างเกือบตลอด -> ปลดล็อกตรงนี้
options(parallelly.maxWorkers.localhost = Inf)
plan(multisession, workers = WORKERS)
message("=== parallel workers = ", WORKERS, " (CPU cores ที่มีจริง: ", parallel::detectCores(), ") ===")

stamp <- format(Sys.time(), "%Y%m%d_%H%M")

# ---- FETCH (เหมือน scrape_daily.R) -------------------------------------------
fetch_html <- function(url, attempt = 1L) {
  Sys.sleep(runif(1, CONFIG$sleep_sec, CONFIG$sleep_sec * 1.5))
  resp <- httr::GET(url, httr::user_agent(CONFIG$user_agent))
  code <- httr::status_code(resp)
  if (code == 429 || code >= 500) {
    if (attempt > CONFIG$max_retries) httr::stop_for_status(resp)
    wait <- CONFIG$backoff_sec * attempt
    message("    rate-limit (", code, ") รอ ", wait, "s (", attempt, ")")
    Sys.sleep(wait)
    return(fetch_html(url, attempt + 1L))
  }
  httr::stop_for_status(resp)
  read_html(resp)
}

# ---- LIST PAGE ----------------------------------------------------------------
build_list_url <- function(page) paste0(CONFIG$base_url, "page=", page)

parse_list_page <- function(list_page) {
  anchors <- list_page |> html_elements(xpath = "//a[contains(@href, '/property/')]")
  if (length(anchors) == 0) return(character(0))
  hrefs <- html_attr(anchors, "href")
  hrefs <- hrefs[!is.na(hrefs) & str_detect(hrefs, "/property/[^/]+/[^/]+/[^/]+")]
  unique(url_absolute(hrefs, "https://www.taladnudbaan.com"))
}

scrape_all_list_urls <- function() {
  message("=== SCRAPE LIST (full) ===")
  acc <- list(); p <- 1L
  repeat {
    t0 <- Sys.time()
    urls <- parse_list_page(fetch_html(build_list_url(p)))
    elapsed <- round(as.numeric(difftime(Sys.time(), t0, units = "secs")), 2)
    if (length(urls) == 0) { message("  หน้า ", p, " ว่าง -> จบ"); break }
    acc[[p]] <- urls
    message(sprintf("  หน้า %d -> %d url (%.2fs)", p, length(urls), elapsed))
    p <- p + 1L
    if (p > CONFIG$max_pages) {
      message("  [WARN] ชน max_pages=", CONFIG$max_pages, " -> อาจตัดข้อมูลทิ้ง ตรวจสอบด้วย")
      break
    }
  }
  unique(unlist(acc))
}

# ---- DETAIL (เหมือน scrape_daily.R) -------------------------------------------
field_after <- function(lines, label) {
  i <- which(str_starts(lines, fixed(label)))
  if (length(i) == 0) return(NA_character_)
  i <- i[1]
  same <- lines[i] |> str_remove(fixed(label)) |>
    str_remove("^\\s*[:：]?\\s*") |> str_trim()
  if (nzchar(same)) return(same)
  if (i < length(lines)) return(str_trim(lines[i + 1]))
  NA_character_
}

parse_baht <- function(x) {
  num <- str_extract(x, "[0-9][0-9,]*")
  if (is.na(num)) return(NA_real_)
  as.numeric(str_remove_all(num, ","))
}

empty_row <- function(url) tibble(
  url=url, property_code=NA_character_, property_type=NA_character_,
  member_code=NA_character_, agency_code=NA_character_, type_slug=NA_character_,
  price_baht=NA_real_, price_history=NA_character_,
  province=NA_character_, district=NA_character_, subdistrict=NA_character_,
  deed_type=NA_character_, land_size=NA_character_, usable_area=NA_character_,
  bedrooms=NA_character_, bathrooms=NA_character_, contact_name=NA_character_,
  agency_name=NA_character_, contact_info=NA_character_, source_url=NA_character_,
  posted_date=NA_character_, updated_date=NA_character_, is_auction=NA,
  scraped_at=as.character(Sys.time())
)

parse_detail <- function(url, page) {
  txt   <- html_text2(page)
  lines <- str_split(txt, "\n")[[1]] |> str_trim()
  lines <- lines[nzchar(lines)]

  parts       <- str_split(url, "/")[[1]]
  agency_code <- parts[length(parts)]
  member_code <- parts[length(parts) - 1]
  type_slug   <- parts[length(parts) - 2]

  meta       <- page |> html_element("meta[name='description']") |> html_attr("content")
  meta_parts <- str_split(meta, ",")[[1]] |> str_trim()
  price_baht <- parse_baht(meta[1])
  province   <- if (length(meta_parts) >= 1) meta_parts[length(meta_parts)] else NA
  district   <- if (length(meta_parts) >= 2) meta_parts[length(meta_parts)-1] else NA

  property_type <- page |> html_element("h1") |> html_text2() |> str_trim()
  property_code <- str_match(txt, "รหัสทรัพย์\\s*[:：]\\s*([A-Za-z0-9]+)")[,2]
  fields        <- map_chr(CONFIG$field_labels, ~ field_after(lines, .x))

  source_url   <- page |>
    html_elements(xpath="//a[normalize-space(.)='ดูทรัพย์']") |> html_attr("href") |>
    (\(x) if (length(x)) x[1] else NA_character_)()
  posted_date  <- str_match(txt, "โพสวันที่\\s*([0-9]{1,2} [^ ]+ [0-9]{4})")[,2]
  updated_date <- str_match(txt, "ปรับปรุงวันที่\\s*([0-9]{1,2} [^ ]+ [0-9]{4})")[,2]

  price_history <- NA_character_
  price_tbl <- page |> html_elements("table") |>
    keep(~ any(str_detect(names(html_table(.x)), "ตร\\.")))
  if (length(price_tbl) > 0) {
    df <- html_table(price_tbl[[1]]); names(df) <- str_trim(names(df))
    if (nrow(df) > 0) {
      unit_col <- names(df)[str_detect(names(df), "ตร\\.")][1]
      rows <- lapply(seq_len(nrow(df)), function(i) list(
        date           = if ("วันที่" %in% names(df)) df[["วันที่"]][i] else NA,
        time           = if ("เวลา"   %in% names(df)) df[["เวลา"]][i]   else NA,
        price          = parse_baht(if ("ราคา" %in% names(df)) df[["ราคา"]][i] else NA),
        price_per_unit = if (!is.na(unit_col)) parse_baht(df[[unit_col]][i]) else NA
      ))
      price_history <- as.character(jsonlite::toJSON(rows, auto_unbox = TRUE))
    }
  }

  is_auction <- str_detect(txt, "ข้อมูลการประมูลทรัพย์")

  tibble(
    url=url, property_code=property_code, property_type=property_type,
    member_code=member_code, agency_code=agency_code, type_slug=type_slug,
    price_baht=price_baht, price_history=price_history,
    province=province, district=district, subdistrict=fields[["subdistrict"]],
    deed_type=fields[["deed_type"]], land_size=fields[["land_size"]],
    usable_area=fields[["usable_area"]], bedrooms=fields[["bedrooms"]],
    bathrooms=fields[["bathrooms"]], contact_name=fields[["contact_name"]],
    agency_name=fields[["agency_name"]], contact_info=fields[["contact_info"]],
    source_url=source_url, posted_date=posted_date, updated_date=updated_date,
    is_auction=is_auction, scraped_at=as.character(Sys.time())
  )
}

scrape_one <- function(url) {
  tryCatch(parse_detail(url, fetch_html(url)),
           error = function(e) { message("    ! ", conditionMessage(e)); empty_row(url) })
}

# ---- MAIN ---------------------------------------------------------------------
start_time <- as.numeric(Sys.time())

if (file.exists(CONFIG$pending_rdata)) {
  message("=== พบ pending queue เดิม -> resume ===")
  load(CONFIG$pending_rdata)   # -> full_urls, full_accum
  message(sprintf("  เหลือ %d / เดิม %d (ทำไปแล้ว %d)",
                  length(full_urls), length(full_urls) + length(full_accum),
                  length(full_accum)))
} else {
  all_urls <- scrape_all_list_urls()
  message("=== ทั้งหมด ", length(all_urls), " url (จะ scrape detail ใหม่หมดทุกตัว) ===")
  full_urls  <- all_urls
  full_accum <- list()
}

n_start <- length(full_urls)
message("=== SCRAPE DETAIL (full re-scrape): เหลือ ", n_start, " รายการ, time limit = ",
        TIME_LIMIT_MIN, " นาที/รอบ ===")

n_done <- 0L
while (length(full_urls) > 0) {
  elapsed_min <- (as.numeric(Sys.time()) - start_time) / 60
  if (elapsed_min >= TIME_LIMIT_MIN) {
    message(sprintf("  หมดเวลารอบนี้ (%.1f นาที) -> บันทึก pending แล้วหยุด (เหลือ %d รายการ)",
                    elapsed_min, length(full_urls)))
    break
  }

  batch_size <- min(CONFIG$chunk_size, length(full_urls))
  batch      <- full_urls[seq_len(batch_size)]
  t0         <- Sys.time()

  message(sprintf("  [batch] %d รายการ (ทำไปแล้ว %d/%d, worker=%d) ...",
                  batch_size, n_done, n_start, WORKERS))
  batch_result <- future_map(batch, scrape_one, .options = furrr_options(seed = TRUE))

  full_accum <- c(full_accum, batch_result)
  full_urls  <- full_urls[-seq_len(batch_size)]
  n_done     <- n_done + batch_size

  elapsed_sec <- round(as.numeric(difftime(Sys.time(), t0, units = "secs")), 1)
  message(sprintf("  [batch done] %.1fs (%.2fs/รายการเฉลี่ย) เหลือ %d รายการ",
                  elapsed_sec, elapsed_sec / batch_size, length(full_urls)))

  save(full_urls, full_accum, file = CONFIG$pending_rdata)
  message("  [checkpoint] เซฟ progress แล้ว")
}

save(full_urls, full_accum, file = CONFIG$pending_rdata)

if (length(full_urls) == 0) {
  message("=== SCRAPE ครบทุกรายการแล้ว -> เขียนไฟล์ผลลัพธ์ ===")
  result <- bind_rows(full_accum)
  out_file <- sprintf("taladnudbaan_properties_full_%s.csv", stamp)
  write_excel_csv(result, out_file)
  file.remove(CONFIG$pending_rdata)
  writeLines(out_file, "output_files_full.txt")
  message("บันทึก -> ", out_file, " (", nrow(result), " แถว)")
  message("เทียบกับ taladnudbaan_properties.csv (จาก incremental pipeline) ได้เลยเพื่อ audit")
} else {
  message(sprintf("=== ยังไม่จบ เหลือ %d รายการ -> รัน Rscript scrape_monthly_full.R ซ้ำเพื่อทำต่อ ===",
                  length(full_urls)))
}
