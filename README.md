# CodeBenton / (Lambda Service on VM(EC2))

[![Github Link](https://img.shields.io/badge/GitHub-Repository-181717?style=flat-square&logo=github)](https://github.com/SOFTBANK-Hackaton-Final-Pink/Bento-Worker)

## SoftBank Hackathon 2025（本選） - CodeBento Worker

### プロジェクト概要
- **大会名**: SoftBank Hackathon 2025（本選）
- **開発期間**: 2025.11.22 ~ 2025.12.07（約2週間）
- **参加人数**: 6名
- **プロジェクト説明**: ユーザーが提出したコードをクラウド上で安全に実行するPaaS型コード実行サービス
- **技術スタック**: Python, Linux Shell Script（User Data）, AWS Lambda, Amazon EC2, Auto Scaling Group, Amazon SQS, Docker
- **担当役割**: PM

### 主な担当業務と成果
- ユーザーコードを実行するPaaSサービスのPMとして、要件整理、役割分担、進行管理を担当しました。
- SQSのキュー長や処理遅延などのメトリクスを基に、ワーカーインスタンスを自動的にスケールアウトする構成を設計しました。
- ASG Warm Poolを活用し、事前に起動済みのワーカーインスタンスを待機させることで、急なリクエスト増加にも素早く対応できる構成を導入しました。
- Dockerコンテナの再利用を前提としたワーカー実行アーキテクチャを設計し、コード実行環境の起動コスト削減を図りました。
- その結果、ワーカーインスタンスの準備時間を約3分から約30秒まで短縮し、サービスの応答性向上に貢献しました。


---

## 🔗 負荷テスト実行およびSQS Autoscaling確認 (Notion)
[Notionリンクはこちら](https://www.notion.so/Code-Bento-Lambda-on-VM-1-97c0368c16f782ae99a801726ced78fd?source=copy_link)

---

## 🏗 アーキテクチャ (Architecture)
<img width="5664" height="3908" alt="image (1) (1)" src="https://github.com/user-attachments/assets/d7013f35-ead3-4eaa-9a56-b1464906216e" />

## 💻 サイト画面 (Site Image)
<img width="1876" height="917" alt="image (2)" src="https://github.com/user-attachments/assets/978e2baa-51b8-4bf1-8ec2-9dc047476055" />

---

## 🍱 CodeBento テーマ

> ### コード弁当
> 顧客が独自のコード弁当を作成して応答を受けることができるサービス

---

## 💡 Pinkチームが考えた 'Run Functions Instantly over HTTP' に対する解釈

> サーバーレス方式の水平拡張モデルを再現することが核心だと思いました。
> そこで、私たちPinkチームは、VM + Container + Message Queue の組み合わせで、これを再現してみようとしました。

**【Pinkチームの立場から考えたコアアイデア】**
- **CodeBentoサーバーをコスト効率よく運営する**
  - AWS ASG Warm-pool方式でサーバーコールドスタートを最小化
- **ピーク時に集中する負荷を非同期で応答する**
  - QueueとDocker Container Warm-pool方式でコンテナコールドスタートを最小化

---

## ✨ Code Bentoのアピールポイント

### 3つのケースに効率的に対応できる。

> - SQSに臨界点以上にメッセージが多く蓄積される場合
> - EC2サーバーのCPUが過負荷になる場合
> - 特定の時間帯にトラフィックが集中する場合

---

## 📊 負荷テスト (Load Testing)
**1000人のユーザーが段階的に計15000個の関数を送った時を想定して負荷テストを実施**

### 1. Locustでの1000人負荷テスト
<img width="2252" height="1347" alt="image (3)" src="https://github.com/user-attachments/assets/4b005d80-317c-4f34-bc6c-fbee786c1d05" />

### 2. Auto-scaling インスタンス
「中止」と表記されたインスタンスが、今後のオートスケーリング発生時に即座に使用される（Warm Pool待機状態の）インスタンスです。
<img width="1566" height="513" alt="image (2) (1)" src="https://github.com/user-attachments/assets/f9f3d162-e64d-476d-a66b-815b21d4afaf" />

### 3. SQS Message Queue
SQSに蓄積（キューイング）されたメッセージ。
<img width="1074" height="651" alt="image (4)" src="https://github.com/user-attachments/assets/51d8affc-7f44-4822-a813-e712d84211ab" />

### 4. Container Warm-Pool 処理
Container Warm-PoolがSQSから関数を受け取って実行する様子。
<img width="951" height="505" alt="image (5)" src="https://github.com/user-attachments/assets/2730926a-9188-4a09-b5a5-d64cd6ec4ec4" />
