# หากยังไม่มีแพ็กเกจ ให้ติดตั้งก่อน: install.packages("data.table")
library(data.table)

# 1. กำหนดโฟลเดอร์ที่เก็บไฟล์ย่อย (ในที่นี้คือโฟลเดอร์ปัจจุบัน)
# และหาชื่อไฟล์ทั้งหมดที่ตรงกับแพทเทิร์น "output_part_*.csv"
file_list <- list.files(pattern = "^output_part_.*\\.csv$")

# เรียงลำดับชื่อไฟล์ให้ถูกต้อง (เช่น part_1, part_2, ..., part_40)
# ป้องกันปัญหาเรียงตัวเลขผิด เช่น part_10 มาก่อน part_2
file_list <- file_list[order(as.numeric(gsub("[^0-9]", "", file_list)))]

# 2. อ่านทุกไฟล์และรวมเข้าด้วยกันในคำสั่งเดียว (เร็วมาก)
# lapply จะใช้อ่านทุกไฟล์ แล้ว rbindlist จะจับมารวมกันเป็นตารางเดียว
combined_df <- rbindlist(lapply(file_list, fread))

# 3. บันทึกกลับเป็นไฟล์ใหญ่ไฟล์เดียว
output_large_file <- "merged_large_file.csv"
fwrite(combined_df, output_large_file)

print(paste("รวมไฟล์เสร็จเรียบร้อย! จำนวนแถวทั้งหมด:", nrow(combined_df)))