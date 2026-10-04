# =============================================================================
# list_sliced.R  (ใช้ร่วมกันโดย scrape_monthly_full.R และ scrape_daily.R)
# ต้อง source() หลังนิยาม: CONFIG$list_base_url, fetch_html(), parse_list_page(), WORKERS
# และโหลด dplyr/stringr/purrr/rvest/future/furrr แล้ว
# =============================================================================

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

# ชื่อจังหวัดที่พบมากสุดในการ์ดหน้านี้ ("อำเภอ , จังหวัด <ราคา> บาท") ใช้จับคู่ อำเภอ <-> จังหวัด
parse_list_province <- function(list_page) {
  # การ์ดทรัพย์: "<อำเภอ>, <จังหวัด>" บรรทัดหนึ่ง ตามด้วยบรรทัดราคา "<ตัวเลข> บาท"
  m <- str_match_all(html_text2(list_page), "(?m)^[^\\n,]+,\\s*([^\\n,]+)\\n[0-9,]{4,}\\s*บาท")[[1]][, 2]
  if (length(m) == 0) NA_character_ else str_trim(names(sort(table(m), decreasing = TRUE))[1])
}

list_fetch <- function(e, page_length = 60L, page = 1L, with_prov = FALSE) {
  pg <- fetch_html(list_url(e, page_length, page))
  out <- list(n = parse_list_total(pg), df = parse_list_page(pg))
  if (with_prov) out$prov <- parse_list_province(pg)
  out
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

# แบ่งตาม sale_method ก่อนเสมอ (1 = รายการขาย, 2 = รายการประมูล) -> ได้ป้ายกำกับ sale/auction ตรงจากเว็บ
# ไม่ต้องเดาจาก is_auction ในหน้ารายละเอียด (ทดสอบแล้วตรงกัน ~98%) และแต่ละส่วนเล็กลงทำให้ไล่ลึกน้อยลง
list_scope <- function(e) {
  parts <- lapply(c(1L, 2L), function(sm) {
    r <- list_solve(paste0(e, "&sale_method=", sm))
    r$df$sale_method <- c("sale", "auction")[sm]
    r
  })
  list(df    = bind_rows(lapply(parts, `[[`, "df")) |> distinct(url, .keep_all = TRUE),
       short = do.call(c, lapply(parts, `[[`, "short")))
}

list_city <- function(cid) {
  e <- paste0("&city_id=", cid)
  tryCatch(c(list_scope(e), list(failed = FALSE)),
           error = function(err) {
             message("  [list ERROR] ", e, ": ", conditionMessage(err))
             list(df = tibble(url = character(0), updated_date = character(0), sale_method = character(0)),
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

  probe_one <- function(e) tryCatch({ r <- list_fetch(e, with_prov = TRUE); list(n = r$n, prov = r$prov) },
                                    error = function(err) NULL)
  probe_n <- function(p) vapply(p, function(x) if (is.null(x)) NA_integer_ else x$n, integer(1))
  probe_p <- function(p) vapply(p, function(x) if (is.null(x)) NA_character_ else as.character(x$prov), character(1))

  cprobe <- future_map(paste0("&city_id=", 1:1400), probe_one, .options = opt)
  for (k in which(vapply(cprobe, is.null, logical(1)))) cprobe[[k]] <- probe_one(paste0("&city_id=", k))   # retry probe ที่ error
  cn <- probe_n(cprobe); cprov <- probe_p(cprobe)
  ids <- which(!is.na(cn) & cn > 0L)
  ids <- ids[order(-cn[ids])]                                       # อำเภอใหญ่ก่อน
  message("  อำเภอที่มีทรัพย์ ", length(ids), " แห่ง (รวมตาม filter ", sum(cn[ids]), ")")

  res <- future_map(ids, list_city, .options = opt)
  failed <- ids[vapply(res, function(x) isTRUE(x$failed), logical(1))]
  if (length(failed) > 0) {
    message("  retry อำเภอที่ error ", length(failed), " แห่ง")
    res[match(failed, ids)] <- future_map(failed, list_city, .options = opt)
  }

  # รอบเก็บตก: ทรัพย์ที่ไม่ผูกกับอำเภอใดเลย (หรืออำเภอที่ยังขาด) filter อำเภอจับไม่ได้
  # -> เทียบ N ราย จังหวัด กับผลรวม N ของอำเภอในจังหวัดนั้น จังหวัดไหนเหลือ -> แบ่ง/ไล่ทั้งจังหวัดแล้ว union
  pids   <- 1:150
  pprobe <- future_map(paste0("&province_id=", pids), probe_one, .options = opt)
  for (k in which(vapply(pprobe, is.null, logical(1)))) pprobe[[k]] <- probe_one(paste0("&province_id=", k))
  pn <- probe_n(pprobe); pname <- probe_p(pprobe)
  live <- which(!is.na(pn) & pn > 0L)
  covered <- vapply(live, function(i) {
    if (is.na(pname[i])) return(0L)
    sum(cn[ids][!is.na(cprov[ids]) & cprov[ids] == pname[i]])
  }, numeric(1))
  resid <- pn[live] - covered
  sweep_p <- pids[live][resid > 0]
  message("  รอบเก็บตก: จังหวัดที่ผลรวมอำเภอยังน้อยกว่ายอดจังหวัด ", length(sweep_p), " แห่ง ขาดรวม ", sum(resid[resid > 0]),
          " (จังหวัด id: ", paste(head(sweep_p, 20), collapse = ","), ")")
  if (length(sweep_p) > 0) {
    pres <- future_map(sweep_p, function(pid) {
      e <- paste0("&province_id=", pid)
      tryCatch(c(list_scope(e), list(failed = FALSE)),
               error = function(err) { message("  [list ERROR] ", e, ": ", conditionMessage(err))
                                       list(df = tibble(url = character(0), updated_date = character(0), sale_method = character(0)),
                                            short = NULL, failed = TRUE) })
    }, .options = opt)
    res <- c(res, pres)
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

  df |> mutate(page = NA_integer_) |> select(url, updated_date, page, sale_method)
}

