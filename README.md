Code Bento 

![image.png](attachment:a9f2b7ae-4136-4722-9d6f-f9b8c789e0d3:image.png)

[Github Link →](https://github.com/SOFTBANK-Hackaton-Final-Pink/Bento-Worker)

Arcitecture

![image (1) (1).png](attachment:a9d64649-87bc-47e8-8d06-33cd99118bc4:image_(1)_(1).png)

## **CodeBento テーマ**

<aside>

### コード弁当 ****🍱

顧客が独自のコード弁当を作成して応答を受けることができるサービス

</aside>

---

## Pinkチームが考えた'Run Functions Instantly over HTTP'に対する解釈

> サーバーレス方式の水平拡張モデルを再現することが核心だと思いました。
そこで、私たちPinkチームは、VM + Container + Message Queue の組み合わせで、これを再現してみようとしました。
> 

PINKチームの立場から考えたコアアイデア

- CodeBentoサーバーをコスト効率よく運営する
    - AWS ASG Warm-pool方式でサーバーコールドスタートを最小化
- ピーク時に集中する負荷を非同期で応答する
    - QueueとDocker Container Warm-pool方式でコンテナコールドスタートを最小化

---

## Code Bentoの アピール·ポイント

### 3 つのケースに効率的に対応できる。

<aside>

- SQSに臨界点以上にメッセージが多く蓄積される場合
- EC2サーバーのCPUが過負荷になる場合
- 特定の時間帯にトラフィックが集中する場合
</aside>

---

### 1000人のユーザーが段階的に計15000個の関数を送った時を想定して負荷テストを実施

Locustで1000人負荷

![image.png](attachment:e235f851-c8d8-4533-a661-6ae62622029f:image.png)

中止と表記されたインスタンスが今後のautoscalingにすぐに使用されるインスタンスです

![image (2).png](attachment:01d6fa9e-9a25-4a0f-b68c-2bdc5f3221b0:image_(2).png)

SQSにたまったメッセージ

![image.png](attachment:75c00f9d-bbce-4040-9ff8-90d1c7f56327:image.png)

![image.png](attachment:35838e68-3e0f-4af4-ac75-1f67c890c584:image.png)

Container Warm-PoolがSQSで関数を受け取って実行する様子
