# =============================================================================
# scrape_daily.R
# รันทุกวัน 00:00 ICT (GitHub Actions)
# 1) scrape list ใหม่ทั้งหมด
# 2) diff vs baseline -> new / removed / updated
# 3) scrape detail เฉพาะ new + updated (ทีละ chunk เช็คเวลา)
# output:
#   list_YYYYMMDD_HHMM.csv
#   changelog_YYYYMMDD_HHMM.csv
#   detail_update_YYYYMMDD_HHMM.csv
# =============================================================================

# ---- CONFIG -----------------------------------------------------------------
CONFIG <- list(
  
  base_url  = "https://www.taladnudbaan.com/properties?sellers_only_out=on&order=price%20desc&view=list&page_length=60&",
  sleep_sec        = 0.1,
  max_retries      = 4L,
  backoff_sec      = 20,
  max_pages        = 9000L,
  chunk_size       = 1000L,   # scrape detail ทีละกี่รายการ
  time_limit_min   = 250L,    # หยุดถ้าผ่านไปแล้วกี่นาทีนับจากเริ่ม workflow
  flicker_threshold = 2L,     # ต้องเห็น "หายจาก list" ติดกันกี่รอบถึงจะเชื่อว่า removed จริง (กัน race condition/flicker ตอนไล่ list)
  
  # input files (จาก scrape ครั้งแรก)
  baseline_list_rdata = "taladnudbaan_urls.RData",     # url_df (page, url, updated_date)
  pending_rdata        = "taladnudbaan_pending.RData",  # cache รายการค้าง (สำหรับ resume run ทุก 6 ชม.)
  
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


pkgs <- c("rvest","dplyr","stringr","purrr","readr","httr","jsonlite","future","furrr")
missing <- pkgs[!vapply(pkgs, requireNamespace, logical(1), quietly = TRUE)]
if (length(missing) > 0) stop("ติดตั้ง packages ก่อน: install.packages(c(",
                              paste(sprintf('"%s"', missing), collapse=", "),"))")
library(rvest); library(dplyr); library(stringr); library(purrr); library(readr)
library(future); library(furrr)

WORKERS <- as.integer(Sys.getenv("WORKERS", "10"))  # จำนวน request ทำพร้อมกัน (I/O-bound -> ปลอดภัย)
options(parallelly.maxWorkers.localhost = Inf)       # GH Actions runner มีแค่ 4 CPU cores เตี้ยกว่า workers ที่ตั้ง
plan(multisession, workers = WORKERS)
message("=== parallel workers = ", WORKERS, " ===")

stamp <- format(Sys.time(), "%Y%m%d_%H%M")

# RESUME_ONLY=true -> ไม่ scrape list ใหม่ ทำต่อจาก pending queue เดิม (ใช้กับ run ทุก 6 ชม. ระหว่างวัน)
RESUME_ONLY <- toupper(Sys.getenv("RESUME_ONLY", "false")) == "TRUE"

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
# baseline_urls ต้องมีคอลัมน์ missed_count (จำนวนรอบติดกันที่ "หายจาก list") อยู่แล้ว
# ป้องกัน flicker: url ที่หายไปแค่ 1 รอบ (อาจแค่ขยับหน้าเพราะ list เป็น dynamic ระหว่างไล่
# ~2,700 หน้า) จะยังไม่ถูกตัดออกจาก baseline จนกว่าจะหายติดกันครบ flicker_threshold รอบ
diff_lists <- function(baseline_urls, baseline_detail, new_list, flicker_threshold = 2L) {
  base_url <- baseline_urls$url
  new_url  <- new_list$url

  new_urls    <- setdiff(new_url, base_url)
  missing_now <- setdiff(base_url, new_url)
  common_urls <- intersect(base_url, new_url)

  miss_map <- setNames(baseline_urls$missed_count, baseline_urls$url)
  miss_map[missing_now] <- miss_map[missing_now] + 1L
  miss_map[common_urls] <- 0L   # เจอแล้ว -> reset นับใหม่

  removed_urls <- missing_now[miss_map[missing_now] >= flicker_threshold]
  flicker_urls <- setdiff(missing_now, removed_urls)  # หายแต่ยังไม่ครบรอบ -> ยังไม่ตัดออกจาก baseline

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
       flicker_urls = flicker_urls,
       updated_urls = updated_urls,
       changelog    = changelog,
       miss_map     = miss_map)  # url -> missed_count ใหม่ (ครอบคลุม common + missing_now)
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

scrape_details <- function(urls) {
  message(sprintf("  scrape %d รายการ (worker=%d) ...", length(urls), WORKERS))
  bind_rows(future_map(urls, scrape_one, .options = furrr_options(seed = TRUE)))
}

# ---- MAIN -------------------------------------------------------------------
if (!file.exists(CONFIG$baseline_list_rdata))
  stop("ไม่พบ baseline list: ", CONFIG$baseline_list_rdata)

message("โหลด baseline list...")
load(CONFIG$baseline_list_rdata)   # -> url_df (page, url, updated_date, missed_count, detail_ok)

# รองรับ baseline เก่าที่ยังไม่มีคอลัมน์ใหม่ (missed_count, detail_ok) -> เติม default
if (!"missed_count" %in% names(url_df)) url_df$missed_count <- 0L
if (!"detail_ok"    %in% names(url_df)) url_df$detail_ok    <- TRUE

# normalize updated_date format เพื่อ match กับ list page ใหม่
url_df <- url_df |> mutate(updated_date = normalize_date(updated_date))
baseline_detail <- url_df |> select(url, updated_date)

if (RESUME_ONLY) {
  message("=== RESUME MODE: ทำต่อจาก pending queue (ไม่ scrape list ใหม่) ===")
  if (!file.exists(CONFIG$pending_rdata)) {
    message("ไม่มี pending queue -> ไม่มีอะไรต้องทำ")
    writeLines(character(0), "output_files.txt")
    writeLines("no_pending", "update_status.txt")
    quit(save = "no", status = 0)
  }
  load(CONFIG$pending_rdata)   # -> pending_urls, pending_flagged, pending_changelog
  to_scrape_all <- pending_urls
  flagged       <- pending_flagged
  changelog_all <- pending_changelog
  message(sprintf("  pending=%d รายการ", length(to_scrape_all)))
} else {
  # scrape list ใหม่
  new_list <- scrape_all_list()

  # diff
  message("=== DIFF ===")
  d <- diff_lists(url_df, baseline_detail, new_list, flicker_threshold = CONFIG$flicker_threshold)
  message(sprintf("  new=%d  removed=%d  updated=%d  flicker(หายแต่ยังไม่ครบรอบ)=%d",
                  length(d$new_urls), length(d$removed_urls), length(d$updated_urls), length(d$flicker_urls)))

  # url ที่เคย scrape detail แล้ว fail (empty_row) ครั้งก่อน -> retry ทุกครั้งไม่ว่า updated_date จะตรงกันไหม
  # (ถ้าไม่ทำแบบนี้ url ที่ scrape fail จะไม่ถูก re-scrape อีกเลยตราบใดที่ site ไม่ขยับ updated_date)
  retry_failed <- url_df$url[!url_df$detail_ok & url_df$url %in% new_list$url]
  if (length(retry_failed) > 0) message("  retry รายการที่เคย scrape fail: ", length(retry_failed))

  # flagged list
  flagged <- new_list |>
    mutate(status = case_when(
      url %in% d$new_urls     ~ "new",
      url %in% d$updated_urls ~ "updated",
      url %in% retry_failed   ~ "retry",
      TRUE                    ~ "unchanged"
    )) |>
    bind_rows(
      url_df |> filter(url %in% d$removed_urls) |>
        transmute(url, updated_date = baseline_detail$updated_date[match(url, baseline_detail$url)],
                  list_page = page, status = "removed"),
      url_df |> filter(url %in% d$flicker_urls) |>
        transmute(url, updated_date = baseline_detail$updated_date[match(url, baseline_detail$url)],
                  list_page = page, status = "flicker")
    ) |>
    mutate(as_of = stamp)

  changelog_all <- d$changelog
  to_scrape_all <- unique(c(d$new_urls, d$updated_urls, retry_failed))

  # ตัด removed ออกจาก baseline เฉพาะที่ "ยืนยันแล้ว" (หายติดกันครบ flicker_threshold รอบ)
  # flicker_urls ยังคงอยู่ใน baseline เหมือนเดิม แค่ missed_count ถูก patch เพิ่มด้านล่าง
  url_df <- url_df |> filter(!url %in% d$removed_urls)

  # patch missed_count ตาม miss_map (ครอบคลุม common_urls ที่ reset เป็น 0 + missing_now ที่ +1)
  miss_updates <- tibble(url = names(d$miss_map), missed_count_new = unname(d$miss_map))
  url_df <- url_df |>
    left_join(miss_updates, by = "url") |>
    mutate(missed_count = coalesce(missed_count_new, missed_count)) |>
    select(-missed_count_new)
}

# scrape detail เฉพาะ new + updated (loop ทีละ chunk_size เช็คเวลาหลังแต่ละ loop)
start_time    <- as.numeric(Sys.getenv("WORKFLOW_START_EPOCH", unset = as.character(as.numeric(Sys.time()))))
detail_done   <- list()
to_scrape_todo <- to_scrape_all  # เริ่มต้น = ยังไม่ได้ทำทั้งหมด

if (length(to_scrape_all) == 0) {
  message("ไม่มีรายการใหม่หรืออัพเดท")
} else {
  chunks <- split(to_scrape_all, ceiling(seq_along(to_scrape_all) / CONFIG$chunk_size))
  message("=== SCRAPE DETAIL: ", length(to_scrape_all), " รายการ | ",
          length(chunks), " chunks x ", CONFIG$chunk_size, " ===")
  for (i in seq_along(chunks)) {
    elapsed_min <- (as.numeric(Sys.time()) - start_time) / 60
    if (elapsed_min >= CONFIG$time_limit_min) {
      message(sprintf("  [chunk %d] หมดเวลา (%.1f นาที >= %d) -> หยุด",
                      i, elapsed_min, CONFIG$time_limit_min))
      break
    }
    message(sprintf("  [chunk %d/%d] %.1f นาทีผ่านไป", i, length(chunks), elapsed_min))
    rows <- scrape_details(chunks[[i]])
    detail_done[[i]] <- rows
    to_scrape_todo <- setdiff(to_scrape_todo, chunks[[i]])
  }
}

detail_update  <- if (length(detail_done) > 0) bind_rows(detail_done) else empty_row(NA)[0, ]
done_urls      <- if (nrow(detail_update) > 0) detail_update$url else character(0)

# output เฉพาะรายการที่ scrape แล้ว
removed_urls_now <- if (RESUME_ONLY) character(0) else d$removed_urls
done_change      <- changelog_all |> filter(url %in% done_urls)
done_flagged     <- flagged       |> filter(url %in% c(done_urls, removed_urls_now))

f_list    <- sprintf("list_%s.csv",          stamp)
f_change  <- sprintf("changelog_%s.csv",     stamp)
f_detail  <- sprintf("detail_update_%s.csv", stamp)

write_excel_csv(done_flagged,  f_list)
write_excel_csv(done_change,   f_change)
write_excel_csv(detail_update, f_detail)
writeLines(c(f_list, f_change, f_detail), "output_files.txt")

# อัพเดท baseline: ที่ scrape แล้ว -> updated_date ใหม่ (patch เข้า url_df เดิม)
# patch updated_date เฉพาะที่ scrape สำเร็จจริง (scrape_ok=TRUE) เท่านั้น -> ถ้า scrape fail
# (404/parse error) updated_date เดิมจะไม่ถูกแตะ ทำให้ diff รอบหน้ายังเห็นว่าต่างจาก site จริง
# ไม่งั้น url ที่ scrape fail ครั้งเดียวจะโดนนับว่า "เสร็จแล้ว" ถาวร ไม่มีวันถูก re-scrape อีก
scrape_ok_map <- detail_update |> distinct(url, .keep_all = TRUE) |> select(url, scrape_ok)
done_dates <- flagged |> filter(url %in% done_urls) |> distinct(url, .keep_all = TRUE) |>
  select(url, updated_date, list_page) |>
  left_join(scrape_ok_map, by = "url")

if (RESUME_ONLY) {
  # resume mode: patch เฉพาะรายการที่ scrape เข้า baseline เดิม ไม่แตะรายการอื่น
  idx   <- match(done_dates$url, url_df$url)
  valid <- !is.na(idx)
  ok    <- valid & !is.na(done_dates$scrape_ok) & done_dates$scrape_ok
  url_df$updated_date[idx[ok]]    <- done_dates$updated_date[ok]
  url_df$detail_ok[idx[valid]]    <- done_dates$scrape_ok[valid]
} else {
  # full mode: มี new_list ครบทุกหน้าอยู่แล้ว -> rebuild url_df จาก flagged (ตัด removed ออกแล้ว)
  new_entries <- done_dates |> filter(!url %in% url_df$url) |>
    transmute(page = list_page, url, updated_date, detail_ok = scrape_ok, missed_count = 0L)
  url_df <- bind_rows(url_df, new_entries)
  idx   <- match(done_dates$url, url_df$url)
  valid <- !is.na(idx)
  ok    <- valid & !is.na(done_dates$scrape_ok) & done_dates$scrape_ok
  url_df$updated_date[idx[ok]] <- done_dates$updated_date[ok]
  url_df$detail_ok[idx[valid]] <- done_dates$scrape_ok[valid]
}
save(url_df, file = CONFIG$baseline_list_rdata)
message("อัพเดท baseline -> ", CONFIG$baseline_list_rdata)

# เก็บ pending queue สำหรับ resume run รอบถัดไป (ทุก 6 ชม.) ถ้ายังมีรายการค้าง
if (length(to_scrape_todo) > 0) {
  pending_urls      <- to_scrape_todo
  pending_flagged   <- flagged       |> filter(url %in% to_scrape_todo)
  pending_changelog <- changelog_all |> filter(url %in% to_scrape_todo)
  save(pending_urls, pending_flagged, pending_changelog, file = CONFIG$pending_rdata)
  message("บันทึก pending queue -> ", CONFIG$pending_rdata, " (", length(to_scrape_todo), " รายการค้าง)")
} else if (file.exists(CONFIG$pending_rdata)) {
  file.remove(CONFIG$pending_rdata)
  message("ไม่มีรายการค้าง -> ลบ pending queue เดิม")
}

n_total     <- length(to_scrape_all)
n_done      <- nrow(detail_update)
n_unupdated <- length(to_scrape_todo)

if (n_unupdated == 0) {
  message("✅ ALL UPDATED")
  update_status <- paste0("all_updated|total:", n_total, "|done:", n_done)
} else {
  message("⏳ UNUPDATED: ยังค้างอยู่ ", n_unupdated, " รายการ")
  update_status <- paste0("unupdated:", n_unupdated, "|total:", n_total, "|done:", n_done)
}
writeLines(update_status, "update_status.txt")
message("เขียน update_status.txt -> ", update_status)