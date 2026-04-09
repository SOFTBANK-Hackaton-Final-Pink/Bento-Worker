# CodeBenton / (Lambda Service on VM(EC2))

[![Github Link](https://img.shields.io/badge/GitHub-Repository-181717?style=flat-square&logo=github)](https://github.com/SOFTBANK-Hackaton-Final-Pink/Bento-Worker)

## 📌 プロジェクト概要
- **プロジェクト説明**: サーバーレスアーキテクチャを活用したインフラ自動デプロイ支援サービス
- **大会名**: SoftBank Hackathon 2025（予選）
- **開発期間**: 2025.11.02 ~ 2025.11.09（1週間）
- **チーム構成**: フロントエンド 1名、バックエンド 2名、PM 1名、インフラ（メンバー全員で担当）
- **技術スタック**: AWS Amplify, Amazon API Gateway, Amazon CloudFront, AWS WAF, Amazon S3, AWS Lambda, Amazon SQS, AWS Step Functions, Amazon CloudWatch, AWS Budgets, TypeScript
- **担当役割**: バックエンド

### 💡 主な担当業務と成果
* **サーバーレスバックエンドおよびRESTful APIエンドポイントの構築**
  * AWS Lambdaを用いてサーバーレスバックエンドロジックを開発し、Amazon API Gatewayと統合しました。
* **S3署名付きURL（Pre-signed URL）を活用したセキュアなファイルアップロード機能の実装**
  * S3の事前署名URLを動的に生成してクライアントに配信するロジックを実装し、安全かつ効率的なファイルアップロード環境を保証しました。
* **非同期処理を用いたイベント駆動型（Event-Driven）アーキテクチャの設計**
  * Amazon API GatewayとLambdaの間にAmazon SQSを結合し、トラフィックを安全にキューイングする非同期処理パイプラインを構築しました。


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
