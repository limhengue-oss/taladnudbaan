# load_secret.R
#
# ดึง secret จาก Windows Credential Manager มาตั้งเป็น env var ให้อัตโนมัติ
# ใช้: source("load_secret.R"); load_secret("GEMINI_API_KEY")
#
# กรณีพิเศษที่จัดการให้อัตโนมัติ:
#   - GCP_SA_KEY / GCP_SERVICE_ACCOUNT_KEY: อ่านจากไฟล์ JSON โดยตรง
#     (ใหญ่เกิน Windows Credential Manager limit ~2560 byte)
#   - ชื่อ env var กับชื่อ credential ไม่ตรงกัน (เช่น globalcpi ใช้ IMF_SDMX_API
#     แต่ credential เก็บไว้ในชื่อ IMF_API_KEY): ระบุ credential_target แยกได้
#
# ที่มา: second-brain/capabilities.md (ทะเบียน key เต็ม + เหตุผลของแต่ละ gotcha)

GCP_SA_KEY_FILE <- "C:/Users/limhe/OneDrive/Documents/GitHub/macroindicator/workfile/macroindicator-6b265-firebase-adminsdk-fbsvc-173fb509b6.json"

load_secret <- function(env_name, credential_target = env_name) {
  if (env_name %in% c("GCP_SA_KEY", "GCP_SERVICE_ACCOUNT_KEY")) {
    if (!file.exists(GCP_SA_KEY_FILE)) {
      stop(sprintf("GCP service account file not found: %s", GCP_SA_KEY_FILE))
    }
    value <- paste(readLines(GCP_SA_KEY_FILE, warn = FALSE), collapse = "\n")
  } else {
    cmd <- sprintf(
      "(Get-StoredCredential -Target '%s').GetNetworkCredential().Password",
      credential_target
    )
    value <- system2("powershell", c("-Command", shQuote(cmd)), stdout = TRUE)
    value <- paste(value, collapse = "\n")
  }

  if (!nzchar(value)) {
    stop(sprintf(
      "Secret '%s' (credential target '%s') is empty or not found in Credential Manager",
      env_name, credential_target
    ))
  }

  do.call(Sys.setenv, setNames(list(value), env_name))
  invisible(value)
}
