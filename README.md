# Code Bento

[여기에 Code Bento 로고/메인 이미지 드래그 앤 드롭]

[Github Link →](https://github.com/SOFTBANK-Hackaton-Final-Pink/Bento-Worker)

## Architecture

[여기에 아키텍처 이미지 드래그 앤 드롭]

---

## **CodeBento テーマ**

> ### コード弁当 🍱
> 顧客が独自のコード弁当を作成して応答を受けることができるサービス

---

## Pinkチームが考えた'Run Functions Instantly over HTTP'に対する解釈

> サーバーレス方式の水平拡張モデルを再現することが核心だと思いました。
> そこで、私たちPinkチームは、VM + Container + Message Queue の組み合わせで、これを再現してみようとしました。

**PINKチームの立場から考えたコアアイデア**
- **CodeBentoサーバーをコスト効率よく運営する**
  - AWS ASG Warm-pool方式でサーバーコールドスタートを最小化
- **ピーク時に集中する負荷を非同期で応答する**
  - QueueとDocker Container Warm-pool方式でコンテナコールドスタートを最小化

---

## Code Bentoの アピール·ポイント

### 3 つのケースに効率的に対応できる。

> - SQSに臨界点以上にメッセージが多く蓄積される場合
> - EC2サーバーのCPUが過負荷になる場合
> - 特定の時間帯にトラフィックが集中する場合

---

## 負荷テスト (Load Testing)
**1000人のユーザーが段階的に計15000個の関数を送った時を想定して負荷テストを実施**

### 1. Locustで1000人負荷
[여기에 Locust 부하 테스트 이미지 드래그 앤 드롭]

### 2. Auto-scaling 인스턴스
中止と表記されたインスタンスが今後のautoscalingにすぐに使用されるインスタンスです。
[여기에 인스턴스 중지/대기 이미지 드래그 앤 드롭]

### 3. SQS Message Queue
SQSにたまったメッセージ
[여기에 SQS 메시지 축적 이미지 드래그 앤 드롭]

### 4. Container Warm-Pool 처리
Container Warm-PoolがSQSで関数を受け取って実行する様子
[여기에 컨테이너 실행 로그 이미지 드래그 앤 드롭]
