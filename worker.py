import boto3
import docker
import time
import json
import requests
import sys
import pymysql
import uuid
import threading
import signal
from datetime import datetime

# ==============================================================================
# ⚙️ [설정]
# ==============================================================================

SQS_QUEUE_URL = 'https://sqs.ap-northeast-2.amazonaws.com/445567111280/MyLambda-Worker-Queue'
REGION = 'ap-northeast-2'

DB_HOST = "rds-lambda.clwq2emuq7jw.ap-northeast-2.rds.amazonaws.com"
DB_USER = "admin"
DB_PASS = "StrongPass123!"
DB_NAME = "pink"

NUM_WORKERS = 10 

RUNTIME_IMAGES = {
    "python": "my-lambda-python:latest",
    "node": "my-lambda-node:latest"
}

pool_lock = threading.Lock()
WARM_CACHE = {
    "python": [],
    "node": []
}

docker_client = docker.from_env()
running = True

# ==============================================================================
# 🕵️‍♂️ [클래스] 리소스 모니터링
# ==============================================================================
class ResourceMonitor(threading.Thread):
    def __init__(self, container):
        super().__init__()
        self.container = container
        self.stop_event = threading.Event()
        self.max_cpu = 0.0
        self.max_memory_mb = 0.0

    def run(self):
        try:
            while not self.stop_event.is_set():
                try:
                    stats = self.container.stats(stream=False)
                    mem_usage = stats.get('memory_stats', {}).get('usage', 0)
                    mem_mb = mem_usage / (1024 * 1024)
                    if mem_mb > self.max_memory_mb: self.max_memory_mb = mem_mb
                    
                    cpu_stats = stats['cpu_stats']
                    precpu_stats = stats['precpu_stats']
                    cpu_delta = cpu_stats['cpu_usage']['total_usage'] - precpu_stats['cpu_usage']['total_usage']
                    system_delta = cpu_stats['system_cpu_usage'] - precpu_stats['system_cpu_usage']
                    
                    if system_delta > 0 and cpu_delta > 0:
                        num_cpus = cpu_stats.get('online_cpus', 1)
                        cpu_percent = (cpu_delta / system_delta) * num_cpus * 100.0
                        if cpu_percent > self.max_cpu: self.max_cpu = cpu_percent
                except:
                    pass
                time.sleep(0.5)
        except Exception:
            pass

    def stop(self):
        self.stop_event.set()

# ==============================================================================
# 💾 [함수] DB 로직
# ==============================================================================
def get_db_connection():
    return pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, db=DB_NAME, charset='utf8mb4')

def fetch_function_details(func_id_str):
    conn = get_db_connection()
    try:
        binary_id = uuid.UUID(func_id_str).bytes
        with conn.cursor(pymysql.cursors.DictCursor) as cursor:
            sql = "SELECT code, runtime FROM functions WHERE id=%s"
            cursor.execute(sql, (binary_id,))
            return cursor.fetchone()
    except Exception as e:
        print(f"   🔥 DB Fetch Error: {e}")
        return None
    finally:
        conn.close()

def save_result_to_db(execution_id_str, logs, result_val, error_msg, duration_ms, cpu_usage, memory_mb):
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            r_id_bin = uuid.uuid4().bytes
            e_id_bin = uuid.UUID(execution_id_str).bytes
            
            final_output = {
                "logs": logs,
                "result": result_val,
                "error": error_msg
            }
            output_json = json.dumps(final_output, ensure_ascii=False)

            sql_insert = """
                INSERT INTO execution_results 
                (result_id, execution_id, output, cpu_usage, memory_usage_mb, duration_ms, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW())
            """
            cursor.execute(sql_insert, (
                r_id_bin, e_id_bin, output_json, cpu_usage, memory_mb, duration_ms
            ))

            # 🚨 [핵심 수정] DB ENUM 타입에 맞춰 단어 변경 (FAILURE / SUCCESS)
            status = "FAILURE" if error_msg else "SUCCESS"
            
            sql_update = "UPDATE executions SET status = %s, updated_at = NOW() WHERE execution_id = %s"
            cursor.execute(sql_update, (status, e_id_bin))
            
        conn.commit()
        print(f"   ✅ [DB Saved] {execution_id_str} -> {status}")
        
    except Exception as e:
        print(f"   🔥 DB Save Error: {e}")
        conn.rollback()
    finally:
        conn.close()

# ==============================================================================
# 🐳 [함수] 컨테이너 관리
# ==============================================================================
def get_container(runtime):
    with pool_lock:
        if runtime not in WARM_CACHE:
             # 혹시 모를 예외 처리 (DB에 이상한 런타임 값 있을 경우)
             raise Exception(f"Unsupported runtime key: {runtime}")

        if WARM_CACHE[runtime]:
            container = WARM_CACHE[runtime].pop(0)
            try:
                container.reload()
                if container.status == 'running':
                    print(f"   ⚡ Warm Start!")
                    return container
            except:
                pass
    
    print(f"   ❄️ Cold Start...")
    image = RUNTIME_IMAGES.get(runtime)
    return docker_client.containers.run(image, detach=True, ports={'8080/tcp': None})

def return_container(runtime, container):
    with pool_lock:
        if runtime in WARM_CACHE:
            WARM_CACHE[runtime].append(container)

def warm_up_initial():
    print(f"🔥 [Warm Pool] Initializing {NUM_WORKERS} containers...")
    targets = ['python'] * 5 + ['node'] * 5
    for rt in targets:
        try:
            c = docker_client.containers.run(RUNTIME_IMAGES[rt], detach=True, ports={'8080/tcp': None})
            with pool_lock:
                WARM_CACHE[rt].append(c)
        except Exception as e:
            print(f"Warning: Warmup failed: {e}")

# ==============================================================================
# 🚀 [함수] Worker Logic
# ==============================================================================
def process_job(sqs, msg):
    container = None
    monitor_thread = None
    
    try:
        body = json.loads(msg['Body'])
        receipt_handle = msg['ReceiptHandle']
        exec_id = body.get('executionId')
        func_id = body.get('functionId')

        if not exec_id or not func_id:
            sqs.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle)
            return

        print(f"🔹 Job Received! [E_ID: {exec_id}]")

        func_details = fetch_function_details(func_id)
        if not func_details:
            save_result_to_db(exec_id, "", None, "Function Not Found", 0, 0, 0)
            sqs.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle)
            return

        code = func_details['code']
        raw_runtime = func_details['runtime'].lower()

        # 🚨 [수정] DB의 'python:3.9' 등을 'python'으로 매핑
        if 'python' in raw_runtime:
            runtime = 'python'
        elif 'node' in raw_runtime:
            runtime = 'node'
        else:
            runtime = raw_runtime

        try:
            container = get_container(runtime)
        except Exception as e:
            save_result_to_db(exec_id, "", None, f"Container Error: {e}", 0, 0, 0)
            sqs.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle)
            return

        start_time = time.time()
        max_cpu = 0.0
        max_mem = 0.0
        response_data = {}
        error_msg = None

        try:
            container.reload()
            host_port = container.attrs['NetworkSettings']['Ports']['8080/tcp'][0]['HostPort']
            agent_url = f"http://localhost:{host_port}/execute"

            monitor_thread = ResourceMonitor(container)
            monitor_thread.start()

            res = requests.post(agent_url, json={"code": code}, timeout=30)
            response_data = res.json()
            
        except Exception as e:
            error_msg = str(e)
        finally:
            duration_ms = int((time.time() - start_time) * 1000)
            if monitor_thread:
                monitor_thread.stop()
                monitor_thread.join()
                max_cpu = monitor_thread.max_cpu
                max_mem = monitor_thread.max_memory_mb

        logs = response_data.get('output', '')
        result_val = response_data.get('result', '')
        
        save_result_to_db(exec_id, logs, result_val, error_msg, duration_ms, max_cpu, max_mem)

        sqs.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle)
        
        if not error_msg:
            return_container(runtime, container)
        else:
            try: container.remove(force=True)
            except: pass

    except Exception as e:
        print(f"❌ Processing Error: {e}")

def worker_thread_loop():
    sqs = boto3.client('sqs', region_name=REGION)
    while running:
        try:
            response = sqs.receive_message(
                QueueUrl=SQS_QUEUE_URL,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=20
            )
            if 'Messages' in response:
                for msg in response['Messages']:
                    process_job(sqs, msg)
        except Exception as e:
            time.sleep(2)

def handle_signal(signum, frame):
    global running
    print("🛑 Shutting down worker...")
    running = False

if __name__ == "__main__":
    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    warm_up_initial()

    print(f"🚀 [Worker Started] Spawning {NUM_WORKERS} threads...")
    threads = []
    for _ in range(NUM_WORKERS):
        t = threading.Thread(target=worker_thread_loop)
        t.daemon = True
        t.start()
        threads.append(t)

    for t in threads:
        t.join()
