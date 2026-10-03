# =============================================================================
# scrape_monthly_full.R
# รัน manual จาก local เท่านั้น (ไม่ auto, ไม่ผูกกับ GitHub Actions)
#
# วัตถุประสงค์: scrape detail ของ "ทุก url ที่ list บนเว็บตอนนี้" ใหม่หมด
# ไม่พึ่ง diff/baseline เหมือน scrape_daily.R -> ผลลัพธ์ไม่มี drift สะสมข้ามเดือน
#
# 2 โหมด:
#   ปกติ (REBUILD_MAIN ไม่ตั้ง/false): เขียนแค่ taladnudbaan_properties_full_<timestamp>.csv
#     แยกไฟล์ ไม่แตะของจริง -> ใช้ audit/cross-check เทียบกับ incremental pipeline เฉยๆ
#   REBUILD_MAIN=true: ใช้เป็น monthly rebuild ตัวจริงแทน scrape_monthly.R เดิม
#     - เขียนทับ taladnudbaan_properties.csv (backup ไฟล์เดิมไว้ก่อน)
#     - rebuild taladnudbaan_urls.RData (baseline) ใหม่หมดจากผลที่ scrape ได้รอบนี้
#       (missed_count=0, detail_ok ตาม scrape_ok จริง) -> ให้ scrape_daily.R เริ่มต้นสะอาด
#       ทุกเดือน ไม่มี drift สะสม (flicker/removed ค้าง) ข้ามเดือน
#
# งานนี้มี url เป็นแสน -> รันครั้งเดียวไม่จบแน่นอน มี resume ในตัว
# (pending queue เก็บลง taladnudbaan_full_pending.RData, สั่งรันสคริปต์ซ้ำได้เรื่อยๆ
#  จนกว่าจะครบ ค่อยเขียนไฟล์ผลลัพธ์สุดท้าย)
#
# usage:
#   Rscript scrape_monthly_full.R                                    # audit เฉยๆ ไม่แตะของจริง
#   REBUILD_MAIN=true WORKERS=20 Rscript scrape_monthly_full.R        # rebuild ของจริงทั้งระบบ
# =============================================================================

CONFIG <- list(
  base_url    = "https://www.taladnudbaan.com/properties?sellers_only_out=on&order=price%20desc&view=list&page_length=60&",
  list_base_url = "https://www.taladnudbaan.com/properties?sellers_only_out=on&view=list",   # ไม่ส่ง order -> ลำดับ default
  sleep_sec   = 0.1,
  max_retries = 4L,
  backoff_sec = 20,
  max_pages   = 9000L,
  chunk_size  = 500L,

  pending_rdata        = "taladnudbaan_full_pending.RData",   # -> full_urls, full_accum (list ของ tibble ที่ scrape ไปแล้ว)
  properties_file      = "taladnudbaan_properties.csv",
  baseline_list_rdata  = "taladnudbaan_urls.RData",

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
REBUILD_MAIN   <- toupper(Sys.getenv("REBUILD_MAIN", "false")) == "TRUE"

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

# เก็บ url + updated_date (เหมือน scrape_daily.R) ไม่ใช่แค่ url เฉยๆ -> เอาไป rebuild
# baseline (taladnudbaan_urls.RData) ได้ตรงๆ เวลา REBUILD_MAIN=true
parse_list_page <- function(list_page) {
  anchors <- list_page |> html_elements(xpath = "//a[contains(@href, '/property/')]")
  n_anchors <- length(anchors)

  rows <- map(anchors, function(a) {
    href <- html_attr(a, "href")
    if (is.na(href) || !str_detect(href, "/property/[^/]+/[^/]+/[^/]+")) return(NULL)
    card <- tryCatch(
      a |> html_element(xpath = "ancestor::*[contains(., 'ปรับปรุงล่าสุด')][1]"),
      error = function(e) NULL
    )
    txt <- if (is.null(card) || length(card) == 0) "" else html_text2(card)
    tibble(
      url          = url_absolute(href, "https://www.taladnudbaan.com"),
      updated_date = str_match(txt, "ปรับปรุงล่าสุด\\s*([0-9]{1,2} [^ ]+ [0-9]{4})")[, 2]
    )
  })

  empty_schema <- tibble(url = character(0), updated_date = character(0))
  n_matched <- sum(!map_lgl(rows, is.null))
  if (n_anchors == 0 || n_matched == 0) return(empty_schema)

  bind_rows(rows) |> distinct(url, .keep_all = TRUE)
}

# ---- LIST แบบแบ่งส่วน (city -> type -> price) + ตรวจครบกับยอดที่เว็บบอก ------------
# ทำไมไม่ไล่ page=1..N ตรงๆ: เว็บแบ่งหน้าแบบ offset และเรียงตามราคา (ราคาเท่ากันลำดับไม่คงที่
# ข้ามหน้า) -> บางรายการซ้ำที่รอยต่อหน้า บางรายการไม่เคยโผล่ (รอบ 1 ต.ค. ได้ 134,608 จาก 170,847)
# หลักการ: ทุก filter เว็บบอกจำนวนรวม "ผลการค้นหา : N" -> ส่วนที่ N<=60 อยู่หน้าเดียวครบแน่ (ไม่ผ่านรอยต่อ)
# ส่วนที่เกิน 60 แบ่งต่อ ถ้าแบ่งไม่ได้ก็ไล่หลายหน้า + ตัวกรองเสริม แล้วเทียบ unique กับ N ทุกส่วน
# ไม่ส่ง order -> ใช้ลำดับ default ของเว็บ (ทดสอบแล้วซ้ำน้อยกว่าเรียงตามราคา)
LIST_WORKERS      <- as.integer(Sys.getenv("LIST_WORKERS", "10"))
LIST_MIN_COVERAGE <- as.numeric(Sys.getenv("LIST_MIN_COVERAGE", "0.98"))   # ต่ำกว่านี้ = หยุด ไม่เขียนทับของจริง

LIST_LEVELS <- list(
  paste0("&type_id=", 1:9),
  c("&min=0&max=500000", "&min=500000&max=1000000", "&min=1000000&max=2000000",
    "&min=2000000&max=3000000", "&min=3000000&max=4000000", "&min=4000000&max=5000000",
    "&min=5000000&max=5000001", "&min=5000001")
)
# ตัวกรองเสริม: ไม่ได้แบ่งครบทุกรายการ (เช่น ที่ดินไม่มีห้องนอน) ใช้เป็นตัวครอบคลุมเพิ่มแล้ว union เท่านั้น
LIST_COVERS <- c(
  paste0("&bedrooms=", c("studio", "1", "2", "3", "4")),
  paste0("&bathrooms=", 1:4),
  paste0("&total_surface=",  c("1-50", "51-100", "101-200", "201-400", "401-1000")),
  paste0("&living_surface=", c("1-50", "51-100", "101-200", "201-400", "401-1000"))
)

list_url <- function(e, page_length = 60L, page = 1L)
  paste0(CONFIG$list_base_url, e, "&page_length=", page_length, "&page=", page)

parse_list_total <- function(list_page) {
  m <- str_match(html_text2(list_page), "ผลการค้นหา\\s*:\\s*([0-9,]+)")[, 2]
  if (is.na(m)) 0L else as.integer(str_remove_all(m, ","))
}

list_fetch <- function(e, page_length = 60L, page = 1L) {
  pg <- fetch_html(list_url(e, page_length, page))
  list(n = parse_list_total(pg), df = parse_list_page(pg))
}

union_urls <- function(a, b) bind_rows(a, b) |> distinct(url, .keep_all = TRUE)

list_paginated <- function(e, page_length) {
  acc <- list(); p <- 1L
  repeat {
    df <- list_fetch(e, page_length, p)$df
    if (nrow(df) == 0L) break
    acc[[p]] <- df
    p <- p + 1L
    if (p > 400L) break   # กันลูปไม่จบ (ส่วนที่แบ่งแล้วไม่เกินไม่กี่พันรายการ)
  }
  bind_rows(acc) |> distinct(url, .keep_all = TRUE)
}

list_rescue <- function(e, have, n) {
  for (pl in c(60L, 40L, 20L)) {
    have <- union_urls(have, list_paginated(e, pl))
    if (nrow(have) >= n) return(have)
  }
  for (cv in LIST_COVERS) {
    r <- list_fetch(paste0(e, cv))
    if (r$n == 0L) next
    add <- if (r$n <= 60L) r$df else list_paginated(paste0(e, cv), 60L)
    have <- union_urls(have, add)
    if (nrow(have) >= n) return(have)
  }
  have
}

# คืน list(df = url ที่ได้, short = ส่วนที่ unique < N หลังลองทุกวิธีแล้ว)
list_solve <- function(e, lvl = 1L) {
  r <- list_fetch(e)
  n <- r$n
  if (n == 0L) return(list(df = tibble(url = character(0), updated_date = character(0)), short = NULL))

  short <- NULL
  if (n <= 60L) {
    got <- r$df
  } else if (lvl <= length(LIST_LEVELS)) {
    parts <- lapply(LIST_LEVELS[[lvl]], function(s) list_solve(paste0(e, s), lvl + 1L))
    got   <- bind_rows(lapply(parts, `[[`, "df")) |> distinct(url, .keep_all = TRUE)
    short <- do.call(c, lapply(parts, `[[`, "short"))
  } else {
    got <- list_paginated(e, 60L)
  }
  if (nrow(got) < n) got <- list_rescue(e, got, n)
  if (nrow(got) < n) short <- c(short, list(tibble(node = e, n = n, got = nrow(got))))
  list(df = got, short = short)
}

list_city <- function(cid) {
  e <- paste0("&city_id=", cid)
  tryCatch(c(list_solve(e), list(failed = FALSE)),
           error = function(err) {
             message("  [list ERROR] ", e, ": ", conditionMessage(err))
             list(df = tibble(url = character(0), updated_date = character(0)),
                  short = NULL, failed = TRUE, cid = cid)
           })
}

scrape_all_list <- function() {
  message("=== SCRAPE LIST (แบ่งอำเภอ/ประเภท/ราคา, workers=", LIST_WORKERS, ") ===")
  plan(multisession, workers = LIST_WORKERS)
  on.exit(plan(multisession, workers = WORKERS), add = TRUE)
  opt <- furrr_options(seed = TRUE, scheduling = Inf)   # 1 อำเภอ/future แบบ dynamic: อำเภอใหญ่ไม่ทำให้คิวค้าง

  total0 <- list_fetch("")$n
  message("  เว็บบอกรวม ", total0, " รายการ")

  probe_one <- function(i) tryCatch(list_fetch(paste0("&city_id=", i))$n, error = function(e) NA_integer_)
  probe <- future_map_int(1:1400, probe_one, .options = opt)
  for (k in which(is.na(probe))) probe[k] <- probe_one(k)           # retry probe ที่ error
  ids <- which(!is.na(probe) & probe > 0L)
  ids <- ids[order(-probe[ids])]                                    # อำเภอใหญ่ก่อน
  message("  อำเภอที่มีทรัพย์ ", length(ids), " แห่ง (รวมตาม filter ", sum(probe[ids]), ")")

  res <- future_map(ids, list_city, .options = opt)
  failed <- ids[vapply(res, function(x) isTRUE(x$failed), logical(1))]
  if (length(failed) > 0) {
    message("  retry อำเภอที่ error ", length(failed), " แห่ง")
    res[match(failed, ids)] <- future_map(failed, list_city, .options = opt)
  }

  df    <- bind_rows(lapply(res, `[[`, "df")) |> distinct(url, .keep_all = TRUE)
  short <- bind_rows(lapply(res, `[[`, "short"))
  total1 <- list_fetch("")$n
  total  <- max(total0, total1)
  cov    <- nrow(df) / total

  message(sprintf("  ได้ unique %d / เว็บบอก %d (ต้น) %d (ปลาย) = %.2f%%", nrow(df), total0, total1, 100 * cov))
  if (nrow(short) > 0) {
    message("  ส่วนที่ยังไม่ครบหลังลองทุกวิธี: ", nrow(short), " ส่วน ขาดรวม ", sum(short$n - short$got), " (บางส่วนซ้อนกัน)")
    print(head(short[order(-(short$n - short$got)), ], 20))
  }
  if (cov < LIST_MIN_COVERAGE)
    stop(sprintf("list ครอบคลุมแค่ %.2f%% < %.0f%% -> หยุด ไม่เขียนทับข้อมูลจริง", 100 * cov, 100 * LIST_MIN_COVERAGE))

  df |> mutate(page = NA_integer_) |> select(url, updated_date, page)
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
  scraped_at=as.character(Sys.time()), scrape_ok=FALSE
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
    is_auction=is_auction, scraped_at=as.character(Sys.time()), scrape_ok=TRUE
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
  load(CONFIG$pending_rdata)   # -> full_urls, full_accum, full_list_df
  message(sprintf("  เหลือ %d / เดิม %d (ทำไปแล้ว %d)",
                  length(full_urls), length(full_urls) + length(full_accum),
                  length(full_accum)))
} else {
  full_list_df <- scrape_all_list()   # tibble(url, updated_date, page) -> เก็บไว้ rebuild baseline ตอนจบ
  message("=== ทั้งหมด ", nrow(full_list_df), " url (จะ scrape detail ใหม่หมดทุกตัว) ===")
  full_urls  <- full_list_df$url
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

  save(full_urls, full_accum, full_list_df, file = CONFIG$pending_rdata)
  message("  [checkpoint] เซฟ progress แล้ว")
}

save(full_urls, full_accum, full_list_df, file = CONFIG$pending_rdata)

if (length(full_urls) == 0) {
  message("=== SCRAPE ครบทุกรายการแล้ว -> เขียนไฟล์ผลลัพธ์ ===")
  result <- bind_rows(full_accum)
  out_file <- sprintf("taladnudbaan_properties_full_%s.csv", stamp)
  write_excel_csv(result, out_file)
  output_files <- out_file
  message("บันทึก -> ", out_file, " (", nrow(result), " แถว)")

  if (REBUILD_MAIN) {
    message("=== REBUILD_MAIN=true -> เขียนทับของจริงทั้งระบบ ===")

    # 1) backup + เขียนทับ properties.csv หลัก
    if (file.exists(CONFIG$properties_file)) {
      suffix   <- format(file.info(CONFIG$properties_file)$mtime, "%Y%m%d")
      old_name <- str_replace(CONFIG$properties_file, "\\.csv$", paste0("_backup_", suffix, ".csv"))
      file.rename(CONFIG$properties_file, old_name)
      message("  backup properties.csv เดิม -> ", old_name)
    }
    write_excel_csv(result, CONFIG$properties_file)
    message("  เขียนทับ -> ", CONFIG$properties_file, " (", nrow(result), " แถว)")

    # 2) rebuild baseline (taladnudbaan_urls.RData) ใหม่หมดจาก list ที่เพิ่ง scrape
    #    -> missed_count=0 ทุกตัว, detail_ok ตาม scrape_ok จริงของรอบนี้ -> scrape_daily.R
    #    เริ่มต้นสะอาดทุกเดือน ไม่มี flicker/removed ค้างสะสมข้ามเดือน
    scrape_ok_map <- result |> distinct(url, .keep_all = TRUE) |> select(url, scrape_ok)
    url_df <- full_list_df |>
      select(page, url, updated_date) |>
      left_join(scrape_ok_map, by = "url") |>
      mutate(missed_count = 0L, detail_ok = coalesce(scrape_ok, FALSE)) |>
      select(-scrape_ok)
    save(url_df, file = CONFIG$baseline_list_rdata)
    message("  rebuild baseline -> ", CONFIG$baseline_list_rdata, " (", nrow(url_df), " url)")

    output_files <- c(output_files, CONFIG$properties_file, CONFIG$baseline_list_rdata)
  }

  file.remove(CONFIG$pending_rdata)
  writeLines(output_files, "output_files_full.txt")
} else {
  message(sprintf("=== ยังไม่จบ เหลือ %d รายการ -> รัน Rscript scrape_monthly_full.R ซ้ำเพื่อทำต่อ ===",
                  length(full_urls)))
}
