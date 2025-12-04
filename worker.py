import boto3
import docker
import time
import json
import requests
import sys
import traceback

# ==============================================================================
# [설정] AWS SQS URL (사용자분 계정 ID 확인 완료)
# ==============================================================================
SQS_QUEUE_URL = 'https://sqs.ap-northeast-2.amazonaws.com/445567111280/MyLambda-Worker-Queue'
REGION = 'ap-northeast-2'

RUNTIME_IMAGES = {
    "python": "my-lambda-python:latest",
    "node": "my-lambda-node:latest"
}

# ⭐ Warm Pool (캐시) - 실행된 컨테이너를 담아두는 곳
WARM_CACHE = {}

try:
    docker_client = docker.from_env()
    sqs = boto3.client('sqs', region_name=REGION)
except Exception as e:
    print(f"🔥 초기화 실패: {e}")
    sys.exit(1)

# ==============================================================================
# ⚙️ [1단계] Pre-warming (미리 켜두기) 함수
# ==============================================================================
def initialize_warm_pool():
    print(f"\n🔥 [Pre-warming] Initializing Containers...")
    
    # 지원하는 모든 언어(Python, Node)를 하나씩 미리 띄움
    runtimes_to_init = ['python', 'node']
    
    for rt in runtimes_to_init:
        image = RUNTIME_IMAGES[rt]
        # 이미 켜져있는지 확인 (중복 실행 방지)
        if rt in WARM_CACHE:
            continue

        print(f"   creating {rt} container ({image})...")
        try:
            container = docker_client.containers.run(
                image,
                detach=True,
                ports={'8080/tcp': None} # 랜덤 포트
            )
            time.sleep(1) # 부팅 대기
            container.reload() # 상태 갱신
            
            # 캐시에 저장 (이제 얘는 Always On 상태가 됨)
            WARM_CACHE[rt] = container
            print(f"   ✅ {rt} is Ready & Warm!")
            
        except Exception as e:
            print(f"   ⚠️ Pre-warming failed for {rt}: {e}")

# ==============================================================================
# 🚀 메인 로직 시작
# ==============================================================================

print(f"\n🚀 [Worker Started]")
print(f"📡 Target Queue: {SQS_QUEUE_URL}")

# ⭐ 서버 시작하자마자 미리 만들어두기 호출!
initialize_warm_pool()

print("Waiting for jobs... (Press Ctrl+C to stop)\n")

while True:
    try:
        # 1. SQS 메시지 수신 (Long Polling)
        response = sqs.receive_message(
            QueueUrl=SQS_QUEUE_URL,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=20,
            AttributeNames=['ApproximateReceiveCount']
        )

        if 'Messages' not in response:
            continue

        message = response['Messages'][0]
        receipt_handle = message['ReceiptHandle']

        try:
            body = json.loads(message['Body'])
        except:
            # JSON 파싱 실패시 텍스트 그대로 처리 (테스트용)
            body = {"runtime": "python", "code": message['Body']}

        # ✅ [수정된 부분] ID 추출 로직 강화 (Scenario A 대응)
        # 1순위: executionId, 2순위: uuid, 없으면 Unknown
        exec_id = body.get('executionId') or body.get('uuid') or 'Unknown-ID'
        
        # 런타임 정리 (대소문자 무시)
        raw_runtime = body.get('runtime', 'python').lower()
        
        if 'node' in raw_runtime or 'js' in raw_runtime:
            req_runtime = 'node'
            target_image = RUNTIME_IMAGES['node']
        else:
            req_runtime = 'python'
            target_image = RUNTIME_IMAGES['python']

        print(f"🔹 Job Received! [ID: {exec_id}] Runtime: {req_runtime}")

        # ---------------------------------------------------------
        # [2단계 & 3단계] Warm Start vs Cold Start
        # ---------------------------------------------------------
        container = None
        
        # A. Warm Pool 확인
        if req_runtime in WARM_CACHE:
            cached_container = WARM_CACHE[req_runtime]
            try:
                cached_container.reload()
                if cached_container.status == 'running':
                    container = cached_container
                    print(f"   ⚡ Warm Start! (Using Pre-warmed container)")
                else:
                    del WARM_CACHE[req_runtime] # 죽었으면 제거
            except:
                del WARM_CACHE[req_runtime]

        # B. Cold Start (Warm Pool에 없거나 죽었을 때)
        if not container:
            print(f"   ❄️ Cold Start... (Fallback creation)")
            try:
                container = docker_client.containers.run(
                    target_image,
                    detach=True,
                    ports={'8080/tcp': None}
                )
                time.sleep(0.5)
                WARM_CACHE[req_runtime] = container # 다음을 위해 저장
            except Exception as e:
                print(f"   🔥 Creation Failed: {e}")
                # 컨테이너 생성 실패는 재시도(DLQ)를 위해 continue
                continue

        # ---------------------------------------------------------
        # [Active Worker] 실행
        # ---------------------------------------------------------
        processing_success = False
        try:
            container.reload()
            host_port = container.attrs['NetworkSettings']['Ports']['8080/tcp'][0]['HostPort']
            
            agent_url = f"http://localhost:{host_port}/execute"
            code_payload = {"code": body.get("code", "")}
            
            # 실행!
            res = requests.post(agent_url, json=code_payload, timeout=5)
            
            result_data = res.json()
            # 결과 출력
            output_msg = result_data.get('output', '').strip()
            print(f"   ✅ Output: {output_msg}")
            
            processing_success = True

        except Exception as e:
            print(f"   🔥 Exec Error: {e}")
            # 에러난 컨테이너는 폐기처분 (Warm Pool에서도 삭제)
            try:
                container.stop(timeout=1)
                container.remove()
            except:
                pass
            if req_runtime in WARM_CACHE:
                del WARM_CACHE[req_runtime]

        # ---------------------------------------------------------
        # [결과 처리] 성공 시 삭제, 실패 시 보존 (DLQ)
        # ---------------------------------------------------------
        if processing_success:
            sqs.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle)
            print("   🗑️ Job Done. Container kept alive for Warm Pool.\n")
        else:
            print("   ⚠️ Job Failed. Message NOT Deleted (Will retry).\n")

    except KeyboardInterrupt:
        print("\n🛑 Stopping... Cleaning up containers...")
        for rt, c in WARM_CACHE.items():
            try:
                c.stop()
                c.remove()
            except:
                pass
        break
    except Exception as e:
        print(f"System Error: {e}")
        time.sleep(1)
