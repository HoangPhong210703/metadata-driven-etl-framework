import yagmail
import csv
import os
from dotenv import load_dotenv

# Tải các biến môi trường từ tệp .env ở thư mục gốc của dự án
dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
load_dotenv(dotenv_path=dotenv_path)

# --- Lấy thông tin đăng nhập từ biến môi trường ---
SENDER_EMAIL = os.environ.get("SENDER_EMAIL")
SENDER_PASSWORD = os.environ.get("SENDER_PASSWORD")

def get_active_recipients(alert_type_to_find):
    """
    Đọc file CSV và lấy danh sách email người nhận đang hoạt động.
    """
    recipients = []
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'alert_config.csv')
    try:
        with open(config_path, mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                if row['alert_type'] == alert_type_to_find and row.get('active') == '1':
                    # Xử lý cả dấu phẩy và dấu chấm phẩy làm dấu phân cách
                    # Thay thế tất cả dấu phẩy bằng dấu chấm phẩy, sau đó tách bằng dấu chấm phẩy
                    emails = [email.strip() for email in row['recipients'].replace(',', ';').split(';') if email.strip()]
                    recipients.extend(emails)
    except FileNotFoundError:
        print(f"Lỗi: Không tìm thấy tệp cấu hình tại '{config_path}'")
        return []
    except Exception as e:
        print(f"Lỗi khi đọc tệp CSV: {e}")
        return []
    return list(set(recipients))

def send_test_email():
    """
    Gửi một email thử nghiệm.
    """
    # Kiểm tra xem các biến môi trường đã được thiết lập chưa
    if not SENDER_EMAIL or not SENDER_PASSWORD:
        print("LỖI: Không tìm thấy SENDER_EMAIL hoặc SENDER_PASSWORD trong tệp .env của bạn.")
        print("Vui lòng kiểm tra lại tệp .env ở thư mục gốc của dự án.")
        return

    alert_type = 'pipeline_failure'
    recipients = get_active_recipients(alert_type)
    
    if not recipients:
        print(f"Không tìm thấy người nhận nào đang hoạt động cho loại cảnh báo: '{alert_type}'")
        return

    subject = "[Email Thử Nghiệm] Từ Gemini CLI"
    body = """
    <html>
      <body>
        <p>Xin chào,</p>
        <p>Đây là email thử nghiệm được gửi tự động từ Gemini CLI.</p>
        <p>Thư viện <b>yagmail</b> và <b>python-dotenv</b> đang hoạt động bình thường!</p>
        <p>Trân trọng.</p>
      </body>
    </html>
    """
    
    print(f"Đang chuẩn bị gửi email thử nghiệm từ {SENDER_EMAIL} tới: {', '.join(recipients)}")

    try:
        import keyring
        from keyring.backends import null
        keyring.set_keyring(null.Keyring())
        yag = yagmail.SMTP(SENDER_EMAIL, SENDER_PASSWORD)
        yag.send(
            to=recipients,
            subject=subject,
            contents=body
        )
        print(">>> Gửi email thử nghiệm thành công! Vui lòng kiểm tra hộp thư đến của bạn.")
    except Exception as e:
        print(f"LỖI: Không thể gửi email. Chi tiết: {e}")

if __name__ == '__main__':
    send_test_email()
