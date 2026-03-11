# CodeBenton / (Lambda Service on VM(EC2))

[![Github Link](https://img.shields.io/badge/GitHub-Repository-181717?style=flat-square&logo=github)](https://github.com/SOFTBANK-Hackaton-Final-Pink/Bento-Worker)

## 📌 プロジェクト概要
- **プロジェクト説明**: AWS Lambdaのサーバーレス環境をEC2インスタンス上で再現したプロジェクト
- **大会名**: SoftBank Hackathon 2025（本選）
- **開発期間**: 2025.11.29 ~ 2025.12.07（1週間）
- **チーム構成**: フロントエンド 1名、バックエンド 3名、インフラ 2名
- **技術スタック**: AWS EC2, Auto Scaling Group, SQS, Docker, Python, Linux Shell Script (User Data), AWS Lambda
- **担当役割**: PM（プロジェクトマネージャー） & インフラエンジニア

### 💡 主な担当業務と成果
* **コールドスタートのバイパスおよびコンテナ再利用アーキテクチャの実装**
* **SQSメトリクスに基づく柔軟なオートスケーリングの実装**
* **Warm Poolを活用したインスタンスの即時投入システムの構築**
  * ASG (Auto Scaling Group) のWarm Pool機能を活用し、ワーカーインスタンスのサービス投入時間を**3分から30秒へと大幅に短縮**しました。

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
<img width="1074" height="651" alt="image (4)" src="https://github.com/user-attachments/assets/51d8affc-7f44-4822-a813-e712d84211ab" />

### 3. SQS Message Queue
SQSに蓄積（キューイング）されたメッセージ。
<img width="1566" height="513" alt="image (2) (1)" src="https://github.com/user-attachments/assets/f9f3d162-e64d-476d-a66b-815b21d4afaf" />

### 4. Container Warm-Pool 処理
Container Warm-PoolがSQSから関数を受け取って実行する様子。
<img width="951" height="505" alt="image (5)" src="https://github.com/user-attachments/assets/2730926a-9188-4a09-b5a5-d64cd6ec4ec4" />
