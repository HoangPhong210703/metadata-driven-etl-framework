"""Script test gửi email thử nghiệm để kiểm tra cấu hình alert."""

import csv
import os
from pathlib import Path

import keyring
import yagmail
from dotenv import load_dotenv
from keyring.backends import null

keyring.set_keyring(null.Keyring())

# Tải biến môi trường từ .env ở thư mục gốc dự án
load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env")

SENDER_EMAIL = os.environ.get("SENDER_EMAIL")
SENDER_PASSWORD = os.environ.get("SENDER_PASSWORD")
ALERT_CONFIG_PATH = Path(__file__).parent.parent / "config" / "alert_config.csv"


def get_active_recipients(alert_type: str) -> list[str]:
    """Đọc file CSV và lấy danh sách email người nhận đang hoạt động."""
    if not ALERT_CONFIG_PATH.exists():
        print(f"Lỗi: Không tìm thấy tệp cấu hình tại '{ALERT_CONFIG_PATH}'")
        return []
    recipients = []
    with open(ALERT_CONFIG_PATH, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row["alert_type"] == alert_type and row.get("active", "1").strip() == "1":
                emails = [e.strip() for e in row["recipients"].replace(",", ";").split(";") if e.strip()]
                recipients.extend(emails)
    return list(set(recipients))


def send_test_email() -> None:
    """Gửi email thử nghiệm để kiểm tra cấu hình."""
    if not SENDER_EMAIL or not SENDER_PASSWORD:
        print("LỖI: SENDER_EMAIL hoặc SENDER_PASSWORD chưa được cấu hình trong .env")
        return

    recipients = get_active_recipients("pipeline_failure")
    if not recipients:
        print("Không tìm thấy người nhận nào đang hoạt động cho loại 'pipeline_failure'")
        return

    print(f"Đang gửi email thử nghiệm từ {SENDER_EMAIL} tới: {', '.join(recipients)}")
    try:
        yag = yagmail.SMTP(SENDER_EMAIL, SENDER_PASSWORD)
        yag.send(
            to=recipients,
            subject="[ETL Pipeline] Email Thử Nghiệm",
            contents="<p>Email thử nghiệm từ hệ thống ETL Pipeline. Cấu hình email đang hoạt động bình thường.</p>",
        )
        print(">>> Gửi email thử nghiệm thành công!")
    except Exception as e:
        print(f"LỖI: Không thể gửi email. Chi tiết: {e}")


if __name__ == "__main__":
    send_test_email()
