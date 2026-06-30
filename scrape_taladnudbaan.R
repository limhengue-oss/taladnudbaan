# =============================================================================
# scrape_taladnudbaan.R
# ดึงข้อมูลทรัพย์ taladnudbaan.com ทั้งหมด (list -> detail) -> CSV ไฟล์เดียว
# Phase 1: เก็บลิงก์ -> save .RData | Phase 2: ดึง detail -> เขียน CSV ทุก N หน้า
# resume ได้ + เลือก start_page ได้
# =============================================================================

# ---- CONFIG -----------------------------------------------------------------
CONFIG <- list(
  work_dir    = "C:/Users/wasinr/OneDrive - Bank of Thailand/My Github/taladnudbaan",
  
  base_url    = "https://www.taladnudbaan.com/properties?sellers_only_out=on&order=price%20desc&view=list&page_length=60&",
  sleep_sec   = 0.2,                     # หน่วงต่อ request (มี jitter +0~50%)
  max_retries = 4L,                      # ลองใหม่กี่ครั้งเมื่อโดน 429/5xx
  backoff_sec = 20,                      # รอ backoff_sec * ครั้งที่ลอง เมื่อโดน rate-limit
  max_pages   = 9000L,                   # safety cap (ที่ 60/หน้า จริงมี ~2773 หน้า)
  
  # --- เลือกการทำงาน ---
  use_saved_urls  = TRUE,                # TRUE = โหลด list จาก .RData ที่ save ไว้
  # FALSE = scrape list ใหม่ทั้งหมด
  start_page      = 339L,                  # Phase 2 เริ่มจาก list page ไหน
  export_every    = 1L,                # Phase 2: เขียน CSV ทุกกี่ list page
  checkpoint_every = 500L,              # Phase 1: save .RData ทุกกี่หน้า
  
  output_file = "taladnudbaan_properties.csv",
  urls_rdata  = "taladnudbaan_urls.RData",
  user_agent  = "Mozilla/5.0 (research data collection; contact: limhengue@gmail.com)",
  
  # ชื่อคอลัมน์ -> label ไทยในหน้า detail (label เพี้ยน แก้จุดนี้จุดเดียว)
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

# ---- SETUP (fail fast) ------------------------------------------------------
if (!dir.exists(CONFIG$work_dir)) stop("ไม่พบ work_dir: ", CONFIG$work_dir)
setwd(CONFIG$work_dir)

pkgs <- c("rvest", "dplyr", "stringr", "purrr", "readr", "xml2", "httr", "jsonlite")
missing <- pkgs[!vapply(pkgs, requireNamespace, logical(1), quietly = TRUE)]
if (length(missing) > 0) {
  stop("ติดตั้ง packages ก่อน: install.packages(c(",
       paste(sprintf('"%s"', missing), collapse = ", "), "))")
}
library(rvest); library(dplyr); library(stringr); library(purrr); library(readr)

# ---- FETCH (jitter + backoff) -----------------------------------------------
fetch_html <- function(url, attempt = 1L) {
  Sys.sleep(runif(1, CONFIG$sleep_sec, CONFIG$sleep_sec * 1.5))
  resp <- httr::GET(url, httr::user_agent(CONFIG$user_agent))
  code <- httr::status_code(resp)
  if (code == 429 || code >= 500) {
    if (attempt > CONFIG$max_retries) httr::stop_for_status(resp)
    wait <- CONFIG$backoff_sec * attempt
    message("    rate-limit/server (", code, ") — รอ ", wait, "s แล้วลองใหม่ (", attempt, ")")
    Sys.sleep(wait)
    return(fetch_html(url, attempt + 1L))
  }
  httr::stop_for_status(resp)
  read_html(resp)
}

# ---- LIST PAGE --------------------------------------------------------------
build_list_url <- function(page) paste0(CONFIG$base_url, "page=", page)

get_property_urls <- function(list_page) {
  list_page |>
    html_elements(xpath = "//a[contains(@href, '/property/')]") |>
    html_attr("href") |>
    str_subset("/property/[^/]+/[^/]+/[^/]+$") |>
    url_absolute("https://www.taladnudbaan.com") |>
    unique()
}

# ---- DETAIL: helpers --------------------------------------------------------
field_after <- function(lines, label) {
  i <- which(str_starts(lines, fixed(label)))
  if (length(i) == 0) return(NA_character_)
  i <- i[1]
  same <- lines[i] |>
    str_remove(fixed(label)) |>
    str_remove("^\\s*[:：]?\\s*") |>
    str_trim()
  if (nzchar(same)) return(same)
  if (i < length(lines)) return(str_trim(lines[i + 1]))
  NA_character_
}

parse_baht <- function(x) {
  num <- str_extract(x, "[0-9][0-9,]*")
  if (is.na(num)) return(NA_real_)
  as.numeric(str_remove_all(num, ","))
}

empty_row <- function(url) {
  tibble(
    url = url, property_code = NA_character_, property_type = NA_character_,
    member_code = NA_character_, agency_code = NA_character_, type_slug = NA_character_,
    price_baht = NA_real_, price_history = NA_character_,
    province = NA_character_, district = NA_character_, subdistrict = NA_character_,
    deed_type = NA_character_, land_size = NA_character_, usable_area = NA_character_,
    bedrooms = NA_character_, bathrooms = NA_character_, contact_name = NA_character_,
    agency_name = NA_character_, contact_info = NA_character_, source_url = NA_character_,
    posted_date = NA_character_, updated_date = NA_character_, is_auction = NA,
    scraped_at = as.character(Sys.time())
  )
}

# ---- DETAIL: parse 1 ทรัพย์ -> tibble 1 แถว --------------------------------
parse_detail <- function(url, page) {
  txt   <- html_text2(page)
  lines <- str_split(txt, "\n")[[1]] |> str_trim()
  lines <- lines[nzchar(lines)]
  
  parts        <- str_split(url, "/")[[1]]
  agency_code  <- parts[length(parts)]
  member_code  <- parts[length(parts) - 1]
  type_slug    <- parts[length(parts) - 2]
  
  meta <- page |> html_element("meta[name='description']") |> html_attr("content")
  meta_parts   <- str_split(meta, ",")[[1]] |> str_trim()
  price_baht   <- parse_baht(meta[1])
  province     <- if (length(meta_parts) >= 1) meta_parts[length(meta_parts)] else NA
  district     <- if (length(meta_parts) >= 2) meta_parts[length(meta_parts) - 1] else NA
  
  property_type <- page |> html_element("h1") |> html_text2() |> str_trim()
  property_code <- str_match(txt, "รหัสทรัพย์\\s*[:：]\\s*([A-Za-z0-9]+)")[, 2]
  
  fields <- map_chr(CONFIG$field_labels, ~ field_after(lines, .x))
  
  source_url <- page |>
    html_elements(xpath = "//a[normalize-space(.)='ดูทรัพย์']") |>
    html_attr("href") |>
    (\(x) if (length(x)) x[1] else NA_character_)()
  
  posted_date  <- str_match(txt, "โพสวันที่\\s*([0-9]{1,2} [^ ]+ [0-9]{4})")[, 2]
  updated_date <- str_match(txt, "ปรับปรุงวันที่\\s*([0-9]{1,2} [^ ]+ [0-9]{4})")[, 2]
  
  # ประวัติราคา: ดึงทุกแถว -> JSON string ในคอลัมน์เดียว
  # format: [{"date":"19 พ.ค. 2569","time":"16:35:09","price":960000,"price_per_unit":36226}, ...]
  price_history <- NA_character_
  price_tbl <- page |>
    html_elements("table") |>
    keep(~ any(str_detect(names(html_table(.x)), "ตร\\.")))
  if (length(price_tbl) > 0) {
    df <- html_table(price_tbl[[1]])
    names(df) <- str_trim(names(df))
    if (nrow(df) > 0) {
      unit_col <- names(df)[str_detect(names(df), "ตร\\.")][1]
      rows <- lapply(seq_len(nrow(df)), function(i) {
        list(
          date           = if ("วันที่" %in% names(df)) df[["วันที่"]][i] else NA,
          time           = if ("เวลา"   %in% names(df)) df[["เวลา"]][i]   else NA,
          price          = parse_baht(if ("ราคา" %in% names(df)) df[["ราคา"]][i] else NA),
          price_per_unit = if (!is.na(unit_col)) parse_baht(df[[unit_col]][i]) else NA
        )
      })
      price_history <- as.character(jsonlite::toJSON(rows, auto_unbox = TRUE))
    }
  }
  
  is_auction <- str_detect(txt, "ข้อมูลการประมูลทรัพย์")
  
  tibble(
    url = url, property_code = property_code, property_type = property_type,
    member_code = member_code, agency_code = agency_code, type_slug = type_slug,
    price_baht = price_baht, price_history = price_history,
    province = province, district = district, subdistrict = fields[["subdistrict"]],
    deed_type = fields[["deed_type"]], land_size = fields[["land_size"]],
    usable_area = fields[["usable_area"]], bedrooms = fields[["bedrooms"]],
    bathrooms = fields[["bathrooms"]], contact_name = fields[["contact_name"]],
    agency_name = fields[["agency_name"]], contact_info = fields[["contact_info"]],
    source_url = source_url, posted_date = posted_date, updated_date = updated_date,
    is_auction = is_auction, scraped_at = as.character(Sys.time())
  )
}

# ---- PHASE 1: discover urls -> save .RData (checkpoint ทุก N หน้า) ----------
discover_urls <- function() {
  if (CONFIG$use_saved_urls && file.exists(CONFIG$urls_rdata)) {
    message("โหลด url list จาก ", CONFIG$urls_rdata)
    load(CONFIG$urls_rdata)              # -> url_df
    message("  โหลดแล้ว: ", nrow(url_df), " ลิงก์ (หน้าสูงสุด ", max(url_df$page), ")")
    return(url_df)
  }
  
  message("=== PHASE 1: ค้นลิงก์ทรัพย์ทุกหน้า (checkpoint ทุก ",
          CONFIG$checkpoint_every, " หน้า) ===")
  
  # resume phase 1 จาก checkpoint ถ้ามี (use_saved_urls=FALSE แต่ไฟล์มีอยู่แล้ว)
  pages <- list()
  p_start <- 1L
  if (file.exists(CONFIG$urls_rdata)) {
    load(CONFIG$urls_rdata)              # -> url_df
    pages <- split(url_df, url_df$page)
    p_start <- max(url_df$page) + 1L
    message("  resume จากหน้า ", p_start, " (มี ", nrow(url_df), " ลิงก์แล้ว)")
  }
  
  p <- p_start
  repeat {
    u <- get_property_urls(fetch_html(build_list_url(p)))
    if (length(u) == 0) {
      message("  หน้า ", p, " ไม่มีลิงก์ — จบ Phase 1")
      break
    }
    pages[[as.character(p)]] <- tibble(page = p, url = u)
    message(sprintf("  หน้า %d -> %d ลิงก์", p, length(u)))
    
    # checkpoint: save ทุก N หน้า
    if (p %% CONFIG$checkpoint_every == 0) {
      url_df <- bind_rows(pages) |> distinct(url, .keep_all = TRUE)
      save(url_df, file = CONFIG$urls_rdata)
      message(sprintf("  [checkpoint] บันทึก %d ลิงก์ ณ หน้า %d -> %s",
                      nrow(url_df), p, CONFIG$urls_rdata))
    }
    
    p <- p + 1L
    if (p > CONFIG$max_pages) { message("  ถึง max_pages — หยุด"); break }
  }
  
  url_df <- bind_rows(pages) |> distinct(url, .keep_all = TRUE)
  save(url_df, file = CONFIG$urls_rdata)
  message("  [final] รวม ", nrow(url_df), " ลิงก์ -> save ", CONFIG$urls_rdata)
  url_df
}

# ---- PHASE 2: scrape details -> เขียน CSV ทุก export_every หน้า ------------
scrape_details <- function(url_df) {
  done <- character(0)
  if (file.exists(CONFIG$output_file)) {
    done <- read_csv(CONFIG$output_file, col_types = cols(.default = "c"))$url
  }
  pages <- sort(unique(url_df$page))
  pages <- pages[pages >= CONFIG$start_page]
  message("=== PHASE 2: ", length(pages), " หน้า (เริ่มหน้า ", CONFIG$start_page,
          ", ข้ามที่ทำแล้ว ", length(done), ") ===")
  
  buf <- list()
  flush <- function() {
    if (length(buf) == 0) return(invisible())
    df <- bind_rows(buf)
    write_excel_csv(df, CONFIG$output_file, append = file.exists(CONFIG$output_file))
    message("    >> เขียน ", nrow(df), " แถว ลง ", CONFIG$output_file)
    buf <<- list()
  }
  
  for (k in seq_along(pages)) {
    pg   <- pages[k]
    urls <- setdiff(url_df$url[url_df$page == pg], done)
    for (u in urls) {
      message("  หน้า ", pg, " | ", u)
      row <- tryCatch(
        parse_detail(u, fetch_html(u)),
        error = function(e) { message("    ! ", conditionMessage(e)); empty_row(u) }
      )
      buf[[length(buf) + 1]] <- row
    }
    if (k %% CONFIG$export_every == 0) flush()
  }
  flush()
}

# ---- MAIN -------------------------------------------------------------------
url_df <- discover_urls()
scrape_details(url_df)
message("เสร็จ -> ", CONFIG$output_file)