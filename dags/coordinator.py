import sys
from datetime import datetime

from airflow import DAG  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore
from airflow.operators.trigger_dagrun import TriggerDagRunOperator  # type: ignore
from airflow.exceptions import AirflowSkipException  # type: ignore

# Nếu bạn vẫn cần import các hàm config cũ để dùng cho mục đích khác, có thể mở comment
# sys.path.insert(0, "/opt/airflow")
# from src.ingestion.config import load_csv_config, get_data_subjects

def coor(**kwargs):
    """Đọc thông tin data_subject và source được truyền từ Button DAG qua tham số conf."""
    dag_run = kwargs["dag_run"]
    
    # Lấy dữ liệu cấu hình được đẩy sang. Dùng dict rỗng để tránh lỗi NoneType.
    conf = dag_run.conf or {}
    subject = conf.get("data_subject")
    source = conf.get("source")
    layer = conf.get("layer", "src2brz")
    
    if not subject:
        print("[coordinator] Không tìm thấy 'data_subject' trong cấu hình. Bỏ qua pipeline.")
        raise AirflowSkipException("No data subject provided in conf — nothing to process")

    # Đóng gói lại thành cấu hình chuẩn để đẩy xuống layer tiếp theo
    config = {"layer": layer, "data_subjects": [subject], "target_dag": f"{layer}_get_config"}
    
    # Nếu có source được chỉ định, thêm vào config
    if source:
        config["source"] = source
        print(f"[coordinator] Kích hoạt tiến trình cho subject: {subject}, source: {source}")
    else:
        print(f"[coordinator] Kích hoạt tiến trình cho subject: {subject}")
    
    return config

# --- Build DAG ---
with DAG(
    dag_id="coordinator",
    description="Orchestrates source-to-bronze pipeline (Triggered by Button DAGs)",
    schedule=None,  # Quan trọng: Để None vì DAG này không tự chạy theo giờ, mà chờ bị gọi
    start_date=datetime(2024, 1, 1),
    catchup=False,
    render_template_as_native_obj=True,
    tags=["orchestration"],
) as dag:

    # Task 1: Xử lý thông tin nhận được
    coor_task = PythonOperator(
        task_id="coor",
        python_callable=coor,
    )

    # Task 2: Gọi DAG get_config và truyền cấu hình vừa tạo qua XCom
    get_config_trigger = TriggerDagRunOperator(
        task_id="get_config_trigger",
        trigger_dag_id="{{ ti.xcom_pull(task_ids='coor')['target_dag'] }}",
        conf="{{ ti.xcom_pull(task_ids='coor') }}",
    )

    # Khai báo luồng chạy: Nhận dữ liệu -> Truyền đi tiếp
    coor_task >> get_config_trigger