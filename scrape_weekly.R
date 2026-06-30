# =============================================================================
# scrape_weekly.R
# รันทุกวันอาทิตย์เที่ยงคืน (หรือกดรันเอง)
# 1) scrape list ใหม่ทั้งหมด
# 2) diff vs baseline -> new / removed / updated (เช็คจาก updated_date)
# 3) scrape detail เฉพาะ new + updated
# output:
#   list_YYYYMMDD.csv        <update scrape list> flagged
#   changelog_YYYYMMDD.csv   สรุปการเปลี่ยนแปลง
#   detail_update_YYYYMMDD.csv  detail ของ new + updated
# =============================================================================

# ---- CONFIG -----------------------------------------------------------------
CONFIG <- list(
  work_dir  = "C:/Users/wasinr/OneDrive - Bank of Thailand/My Github/taladnudbaan",
  
  base_url  = "https://www.taladnudbaan.com/properties?sellers_only_out=on&order=price%20desc&view=list&page_length=60&",
  sleep_sec   = 0.2,
  max_retries = 4L,
  backoff_sec = 20,
  max_pages   = 9000L,
  
  # input files (จาก scrape ครั้งแรก)
  baseline_list_rdata = "taladnudbaan_urls.RData",     # url_df (page, url, updated_date)
  
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

# ---- SETUP ------------------------------------------------------------------
# บนเครื่องตัวเอง: setwd ไป work_dir | บน GitHub Actions: ใช้ working dir จาก actions/checkout แทน
if (Sys.getenv("GITHUB_ACTIONS") != "true") {
  if (!dir.exists(CONFIG$work_dir)) stop("ไม่พบ work_dir: ", CONFIG$work_dir)
  setwd(CONFIG$work_dir)
}

pkgs <- c("rvest","dplyr","stringr","purrr","readr","httr","jsonlite")
missing <- pkgs[!vapply(pkgs, requireNamespace, logical(1), quietly = TRUE)]
if (length(missing) > 0) stop("ติดตั้ง packages ก่อน: install.packages(c(",
                              paste(sprintf('"%s"', missing), collapse=", "),"))")
library(rvest); library(dplyr); library(stringr); library(purrr); library(readr)

stamp <- format(Sys.Date(), "%Y%m%d")

# ---- FETCH ------------------------------------------------------------------
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

# ---- NORMALIZE DATE ---------------------------------------------------------
# handle Excel format change: "22-มิ.ย.-69" -> "22 มิ.ย. 2569"
# vectorized version (no nested if)
normalize_date <- function(x) {
  if (length(x) == 0) return(character(0))
  needs_fix <- !is.na(x) & str_detect(x, "^[0-9]{1,2}-")
  
  # replace all instances
  result <- x
  if (any(needs_fix)) {
    fixed <- str_replace_all(x[needs_fix], "-", " ")
    # extract year and convert
    yrs <- as.integer(str_extract(fixed, "[0-9]+$"))
    yrs_4digit <- ifelse(yrs < 100, yrs + 2500L, yrs)
    fixed <- str_replace(fixed, "[0-9]+$", as.character(yrs_4digit))
    result[needs_fix] <- fixed
  }
  result
}

# ---- LIST PAGE: url + updated_date ------------------------------------------
build_list_url <- function(page) paste0(CONFIG$base_url, "page=", page)

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

  if (n_anchors == 0) {
    message("    [info] หน้านี้ไม่มี anchor /property/ เลย -> ถือว่าจบข้อมูลจริง")
    return(empty_schema)
  }
  if (n_matched == 0) {
    # ผิดปกติ: มี anchor แต่ regex ไม่ผ่านสักตัว -> อาจไม่ใช่หน้าสุดท้ายจริง
    message("    [WARN] พบ ", n_anchors, " anchor แต่ regex ไม่ผ่านเลยสักตัว -> ตรวจ pattern href")
    return(empty_schema)
  }

  bind_rows(rows) |> distinct(url, .keep_all = TRUE)
}

scrape_all_list <- function() {
  message("=== SCRAPE LIST ===")
  acc <- list(); p <- 1L
  repeat {
    t0 <- Sys.time()
    df <- parse_list_page(fetch_html(build_list_url(p)))
    elapsed <- round(as.numeric(difftime(Sys.time(), t0, units = "secs")), 2)
    if (nrow(df) == 0) { message("  หน้า ", p, " ว่าง — จบ"); break }
    df$list_page <- p
    acc[[p]] <- df
    message(sprintf("  หน้า %d -> %d url (%.2fs)", p, nrow(df), elapsed))
    p <- p + 1L
    if (p > CONFIG$max_pages) break
  }
  bind_rows(acc) |> distinct(url, .keep_all = TRUE)
}

# ---- DIFF -------------------------------------------------------------------
diff_lists <- function(baseline_urls, baseline_detail, new_list) {
  base_url <- baseline_urls$url
  new_url  <- new_list$url
  
  new_urls     <- setdiff(new_url, base_url)
  removed_urls <- setdiff(base_url, new_url)
  common_urls  <- intersect(base_url, new_url)
  
  old_dates <- baseline_detail |>
    filter(url %in% common_urls) |>
    select(url, old_updated = updated_date)
  
  updated_urls <- new_list |>
    filter(url %in% common_urls) |>
    select(url, new_updated = updated_date) |>
    left_join(old_dates, by = "url") |>
    filter(!is.na(new_updated) & (is.na(old_updated) | new_updated != old_updated)) |>
    pull(url)
  
  now <- as.character(Sys.time())
  log_new <- tibble(url = new_urls, change_type = "new",
                    old_updated = NA_character_,
                    new_updated = new_list$updated_date[match(new_urls, new_list$url)],
                    changed_at = now)
  
  log_removed <- tibble(url = removed_urls, change_type = "removed",
                        old_updated = baseline_detail$updated_date[match(removed_urls, baseline_detail$url)],
                        new_updated = NA_character_,
                        changed_at = now)
  
  log_updated <- new_list |>
    filter(url %in% updated_urls) |>
    select(url, new_updated = updated_date) |>
    left_join(old_dates, by = "url") |>
    mutate(change_type = "updated", changed_at = now)
  
  changelog <- bind_rows(log_new, log_removed, log_updated)
  
  list(new_urls     = new_urls,
       removed_urls = removed_urls,
       updated_urls = updated_urls,
       changelog    = changelog)
}

# ---- DETAIL -----------------------------------------------------------------
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

scrape_details <- function(urls) {
  n <- length(urls)
  bind_rows(imap(urls, function(u, i) {
    message(sprintf("  [%d/%d] %s", i, n, u))
    tryCatch(parse_detail(u, fetch_html(u)),
             error = function(e) { message("    ! ", conditionMessage(e)); empty_row(u) })
  }))
}

# ---- MAIN -------------------------------------------------------------------
if (!file.exists(CONFIG$baseline_list_rdata))
  stop("ไม่พบ baseline list: ", CONFIG$baseline_list_rdata)

message("โหลด baseline list...")
load(CONFIG$baseline_list_rdata)   # -> url_df (page, url, updated_date)

# normalize updated_date format เพื่อ match กับ list page ใหม่
url_df <- url_df |> mutate(updated_date = normalize_date(updated_date))
baseline_detail <- url_df |> select(url, updated_date)

# scrape list ใหม่
new_list <- scrape_all_list()

# diff
message("=== DIFF ===")
d <- diff_lists(url_df, baseline_detail, new_list)
message(sprintf("  new=%d  removed=%d  updated=%d",
                length(d$new_urls), length(d$removed_urls), length(d$updated_urls)))

# flagged list
flagged <- new_list |>
  mutate(status = case_when(
    url %in% d$new_urls     ~ "new",
    url %in% d$updated_urls ~ "updated",
    TRUE                    ~ "unchanged"
  )) |>
  bind_rows(
    url_df |> filter(url %in% d$removed_urls) |>
      transmute(url, updated_date = baseline_detail$updated_date[match(url, baseline_detail$url)],
                list_page = page, status = "removed")
  ) |>
  mutate(as_of = stamp)

# scrape detail เฉพาะ new + updated
to_scrape <- unique(c(d$new_urls, d$updated_urls))
if (length(to_scrape) > 0) {
  message("=== SCRAPE DETAIL: ", length(to_scrape), " รายการ ===")
  detail_update <- scrape_details(to_scrape)
} else {
  message("ไม่มีรายการใหม่หรืออัพเดท")
  detail_update <- empty_row(NA)[0, ]
}

# เขียนไฟล์ output
f_list    <- sprintf("list_%s.csv",          stamp)
f_change  <- sprintf("changelog_%s.csv",     stamp)
f_detail  <- sprintf("detail_update_%s.csv", stamp)

write_excel_csv(flagged,       f_list)
write_excel_csv(d$changelog,   f_change)
write_excel_csv(detail_update, f_detail)

# อัพเดท baseline: url_df ใหม่ เก็บ updated_date จาก list page
url_df <- flagged |>
  filter(status != "removed") |>
  transmute(page = list_page, url, updated_date)
save(url_df, file = CONFIG$baseline_list_rdata)
message("อัพเดท baseline -> ", CONFIG$baseline_list_rdata)

message("=== เสร็จ ===")
message("  ", f_list,   " (", nrow(flagged),       " แถว)")
message("  ", f_change, " (", nrow(d$changelog),   " แถว)")
message("  ", f_detail, " (", nrow(detail_update), " แถว)")