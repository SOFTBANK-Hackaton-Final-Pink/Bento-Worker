## 부하테스트 실행 및 SQS Autoscaling 확인 노션 링크
https://www.notion.so/Code-Bento-Lambda-on-VM-1-97c0368c16f782ae99a801726ced78fd?source=copy_link

## Architecture
<img width="5664" height="3908" alt="image (1) (1)" src="https://github.com/user-attachments/assets/d7013f35-ead3-4eaa-9a56-b1464906216e" />


## 사이트 이미지
<img width="1876" height="917" alt="image (2)" src="https://github.com/user-attachments/assets/978e2baa-51b8-4bf1-8ec2-9dc047476055" />

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
<img width="2252" height="1347" alt="image (3)" src="https://github.com/user-attachments/assets/4b005d80-317c-4f34-bc6c-fbee786c1d05" />


### 2. Auto-scaling 인스턴스
中止と表記されたインスタンスが今後のautoscalingにすぐに使用されるインスタンスです。
<img width="1074" height="651" alt="image (4)" src="https://github.com/user-attachments/assets/51d8affc-7f44-4822-a813-e712d84211ab" />


### 3. SQS Message Queue
SQSにたまったメッセージ
<img width="1566" height="513" alt="image (2) (1)" src="https://github.com/user-attachments/assets/f9f3d162-e64d-476d-a66b-815b21d4afaf" />


### 4. Container Warm-Pool 처리
Container Warm-PoolがSQSで関数を受け取って実行する様子
<img width="951" height="505" alt="image (5)" src="https://github.com/user-attachments/assets/2730926a-9188-4a09-b5a5-d64cd6ec4ec4" />


