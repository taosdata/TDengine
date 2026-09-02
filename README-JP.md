<p align="center">
  <a href="https://tdengine.com" target="_blank">
  <img
    src="tdengine-logo.svg"
    alt="TDengine"
    width="500"
  />
  </a>
</p>

<div align="center">
  <a href="https://github.com/taosdata/TDengine/actions/workflows/tdengine-release-build.yml"><img src="https://github.com/taosdata/TDengine/actions/workflows/tdengine-release-build.yml/badge.svg" alt="TDengine Release Build" /></a>
  <a href="https://coveralls.io/github/taosdata/TDengine?branch=3.0"><img src="https://coveralls.io/repos/github/taosdata/TDengine/badge.svg?branch=3.0" alt="Coverage Status" /></a>
  <a href="https://github.com/taosdata/TDengine/commits/main/"><img src="https://img.shields.io/github/commit-activity/m/taosdata/tdengine" alt="GitHub commit activity" /></a>
  <br />
  <a href="https://github.com/taosdata/TDengine/releases"><img src="https://img.shields.io/github/v/release/taosdata/tdengine" alt="GitHub Release" /></a>
  <a href="https://github.com/taosdata/TDengine/blob/main/LICENSE"><img src="https://img.shields.io/github/license/taosdata/tdengine" alt="GitHub License" /></a>
  <a href="https://bestpractices.coreinfrastructure.org/projects/4201"><img src="https://bestpractices.coreinfrastructure.org/projects/4201/badge" alt="CII Best Practices" /></a>
  <br />
  <a href="https://twitter.com/tdenginedb"><img src="https://img.shields.io/twitter/follow/tdenginedb?label=TDengine&style=social" alt="Twitter Follow" /></a>
  <a href="https://www.youtube.com/@tdengine"><img src="https://img.shields.io/badge/Subscribe_@tdengine--white?logo=youtube&style=social" alt="YouTube Channel" /></a>
  <a href="https://discord.com/invite/VZdSuUg4pS"><img src="https://img.shields.io/badge/Join_Discord--white?logo=discord&style=social" alt="Discord Community" /></a>
  <a href="https://www.linkedin.com/company/tdengine"><img src="https://img.shields.io/badge/Follow_LinkedIn--white?logo=linkedin&style=social" alt="LinkedIn" /></a>
  <a href="https://stackoverflow.com/questions/tagged/tdengine"><img src="https://img.shields.io/badge/Ask_StackOverflow--white?logo=stackoverflow&style=social&logoColor=orange" alt="StackOverflow" /></a>
  <a href="https://deepwiki.com/taosdata/TDengine"><img src="https://img.shields.io/badge/Ask%20DeepWiki-white.svg" alt="DeepWiki" /></a>
  <br />
  <a href="./README.md">English</a> |
  <a href="./README-CN.md">简体中文</a> |
  <a href="./README-JP.md">日本語</a> |
  <a href="https://cloud.tdengine.com">TDengine Cloud</a> |
  <a href="https://docs.tdengine.com">ドキュメント</a> |
  <a href="https://tdengine.com/time-series-database/">TSDB についてもっと知る</a>
</div>

# TDengine

## 1. はじめに

TDengine は、IoT（モノのインターネット）、コネクテッドカー、産業用 IoT のために設計された、オープンソースかつ高性能でクラウドネイティブ、そして AI を活用した[時系列データベース](https://tdengine.com/tsdb/)です。数十億のセンサーやデータコレクターが生成する 1 日あたり TB、さらには PB 規模のデータを、効率的にリアルタイムで取り込み、処理、分析できます。TDengine は以下の利点によって他の時系列データベースと一線を画しています:

- **[高性能](https://tdengine.com/tdengine/high-performance-time-series-database/)**: TDengine は、高カーディナリティ問題を解決して数十億のデータ収集ポイントをサポートする唯一の時系列データベースであり、同時にデータの取り込み、クエリ、圧縮において他の時系列データベースを上回る性能を発揮します。

- **[シンプルなソリューション](https://tdengine.com/tdengine/simplified-time-series-data-solution/)**: 組み込みのキャッシュ、ストリーム処理、データ購読、AI エージェント機能により、TDengine は時系列データ処理のためのシンプルなソリューションを提供します。これによりシステム設計の複雑さと運用コストを大幅に削減します。

- **[クラウドネイティブ](https://tdengine.com/tdengine/cloud-native-time-series-database/)**: ネイティブな分散設計、シャーディングとパーティショニング、コンピュートとストレージの分離、RAFT、Kubernetes デプロイのサポート、完全なオブザーバビリティにより、TDengine はクラウドネイティブな時系列データベースであり、パブリック、プライベート、ハイブリッドのいずれのクラウドにもデプロイできます。

- **[AI 活用](https://tdengine.com/tdengine/tdgpt/)**: 組み込みの AI エージェント TDgpt を通じて、TDengine はさまざまな時系列基盤モデル、大規模言語モデル、機械学習、従来型アルゴリズムに接続し、時系列データの予測、異常検知、欠損値補完、分類を提供できます。

- **[使いやすさ](https://tdengine.com/tdengine/easy-time-series-data-platform/)**: 管理者にとっては、TDengine はデプロイと保守の手間を大幅に削減します。開発者にとっては、シンプルなインターフェース、簡潔なソリューション、サードパーティツールとのシームレスな連携を提供します。データ利用者にとっては、容易なデータアクセスを実現します。

- **[容易なデータ分析](https://tdengine.com/tdengine/time-series-data-analytics-made-easy/)**: スーパーテーブル、ストレージとコンピュートの分離、時間間隔によるデータパーティショニング、事前計算、AI エージェントにより、TDengine はデータの探索、整形、アクセスを非常に効率的かつ容易にします。

- **[オープンソース](https://tdengine.com/tdengine/open-source-time-series-database/)**: クラスター機能や AI エージェントを含む TDengine のコアモジュールは、すべてオープンソースライセンスの下で利用できます。GitHub では 23.7k のスターを獲得しており、活発な開発者コミュニティと、世界中で 73 万を超える稼働インスタンスがあります。

TDengine の競争優位性の全リストについては、[こちら](https://tdengine.com/tdengine/)をご覧ください。TDengine を最も簡単に体験する方法は [TDengine Cloud](https://cloud.tdengine.com) を利用することです。最新の TDengine コンポーネントである TDgpt については、[TDgpt README](./tools/tdgpt/README.md) を参照してください。

## 2. ドキュメント

ユーザーマニュアル、システム設計、アーキテクチャについては、[TDengine ドキュメント](https://docs.tdengine.com)（[TDengine 文档](https://docs.taosdata.com)）を参照してください。

TDengine のインストール方法は、[コンテナ](https://docs.tdengine.com/get-started/deploy-in-docker/)、[インストールパッケージ](https://docs.tdengine.com/get-started/deploy-from-package/)、[Kubernetes](https://docs.tdengine.com/operations-and-maintenance/deploy-your-cluster/#kubernetes-deployment) から選択できるほか、インストール不要の[フルマネージドサービス](https://cloud.tdengine.com/)を試すこともできます。このクイックガイドは、TDengine を自分でビルド、リリース、テストし、コントリビュートしたい開発者向けです。

## 目次

- [1. はじめに](#1-はじめに)
- [2. ドキュメント](#2-ドキュメント)
- [3. 前提条件](#3-前提条件)
  - [3.1 システム要件](#31-システム要件)
  - [3.2 ビルドツールのインストール](#32-ビルドツールのインストール)
  - [3.3 オプションのツール](#33-オプションのツール)
- [4. ビルド](#4-ビルド)
  - [4.1 クイックスタート](#41-クイックスタート)
  - [4.2 ビルドオプション](#42-ビルドオプション)
  - [4.3 ビルド成果物](#43-ビルド成果物)
- [5. テスト](#5-テスト)
  - [5.1 ユニットテスト](#51-ユニットテスト)
  - [5.2 統合テスト](#52-統合テスト)
- [6. パッケージング](#6-パッケージング)
  - [6.1 コミュニティ版 tarball のパッケージング](#61-コミュニティ版-tarball-のパッケージング)
- [7. パッケージからのインストール](#7-パッケージからのインストール)
- [8. 実行](#8-実行)
- [9. ワークフロー](#9-ワークフロー)
- [10. カバレッジ](#10-カバレッジ)
- [11. コントリビュート](#11-コントリビュート)
- [12. ライセンス](#12-ライセンス)

## 3. 前提条件

### 3.1 システム要件

- **オペレーティングシステム:** Linux（Ubuntu 18.04 以降、CentOS 7 以降）、macOS 10.15 以降、Windows（限定的。オープンソース版のビルドは主に Linux/macOS が対象）
- **CPU:** x86_64 または ARM64
- **メモリ:** 4 GB RAM 以上を推奨
- **ディスク:** 2 GB 以上の空き容量を推奨
- **主要なビルドプラットフォーム:** Linux

TDengine のビルドとテストは主に Linux 上で行われています。macOS のビルドはローカル開発向けにサポートされています。オープンソースのツリーにおける Windows のサポートは限定的であるため、再現性のあるビルドには Linux をデフォルトの選択肢としてください。

### 3.2 ビルドツールのインストール

**Ubuntu/Debian:**

```bash
sudo apt-get update
sudo apt-get install -y build-essential cmake git
```

**CentOS/RHEL:**

```bash
sudo yum groupinstall -y "Development Tools"
sudo yum install -y cmake3 git
```

注意: 外部ビルドには CMake 3.21 以上が必要です。お使いのディストリビューションが提供するバージョンが古い場合は、[https://cmake.org/download/](https://cmake.org/download/) から新しいリリースをインストールしてください。

ビルドする対象によっては、以下のツールも必要になる場合があります:

- `tests/` のテストフレームワーク用の **Python 3**
- taosAdapter や taosKeeper などのコンポーネントをビルドする場合は **Go 1.23 以上**

### 3.3 オプションのツール

- **ccache** — 再ビルドを高速化します: `sudo apt install ccache`
- **Conan 2.x** — `taos-gen` コンポーネントの場合のみ必要です: `pip3 install conan`

## 4. ビルド

### 4.1 クイックスタート

```bash
git clone https://github.com/taosdata/TDengine.git
cd TDengine
mkdir debug && cd debug
cmake .. -DBUILD_CONTRIB=ON    # 初回ビルド: -DBUILD_CONTRIB=ON は必須です
make -j$(nproc)
```

> **重要**
> 初回のソースビルドでは必ず `-DBUILD_CONTRIB=ON` を使用してください。これにより、xxhash、zstd、lz4 などの外部依存関係がダウンロードされ、`.externals/` にビルドされます。2 回目以降のビルドでは通常このフラグを省略でき、キャッシュされた成果物が自動的に再利用されます。

便利なバリエーション:

```bash
# CLI ツール付きのリリースビルド
cmake .. -DBUILD_CONTRIB=ON -DCMAKE_BUILD_TYPE=Release -DBUILD_TOOLS=ON
make -j$(nproc)

# macOS における同等の並列ビルド
cmake .. -DBUILD_CONTRIB=ON -DCMAKE_BUILD_TYPE=Release
cmake --build . --parallel "$(sysctl -n hw.ncpu)"
```

### 4.2 ビルドオプション

以下のデフォルト値は、特に記載がない限り `cmake/options.cmake` にあるスタンドアロンのオープンソース版のデフォルトです。

| オプション | デフォルト | 説明 |
|---|---:|---|
| `BUILD_CONTRIB` | Linux では `OFF`、それ以外では `ON` | サードパーティの依存関係をソースからビルドします。**初回ビルドでは必須です。** |
| `CMAKE_BUILD_TYPE` | `Debug` | 標準的な CMake のビルドタイプ（`Debug`、`Release`、`RelWithDebInfo`）。 |
| `BUILD_TEST` | `OFF` | googletest によるユニットテストをビルドします。 |
| `BUILD_TOOLS` | `OFF` | `taosBenchmark` や `taosdump` などのツールをビルドします。 |
| `BUILD_SANITIZER` | `OFF` | サニタイザーを有効にします。 |
| `BUILD_COVERAGE` | `OFF` | カバレッジ計測を有効にします。 |
| `BUILD_JEMALLOC` | `OFF` | jemalloc アロケーターのサポートを有効にします。 |
| `BUILD_WEBSOCKET` | `OFF` | WebSocket のサポートを有効にします。 |
| `BUILD_ASSERT_NOT_CORE` | `OFF` | assert 時に core ファイルを生成しないようにします。 |
| `BUILD_PTHREAD_TWEAK` | `OFF` | 古い Linux 環境向けの pthread 調整を適用します。 |
| `BUILD_ASTRA` | `OFF` | Astra プラットフォーム向けにビルドします。 |
| `BUILD_ASTRA_RPC` | `OFF` | Astra RPC トランスポートを使用してビルドします。 |
| `BUILD_WITH_LEMON` | `ON` | lemon パーサーのサポートを含めてビルドします。 |
| `BUILD_WITH_UDF` | `ON` | UDF のサポートを含めてビルドします。 |
| `BUILD_GEOS` | `ON` | GEOS のサポートを含めてビルドします（Astra 以外）。 |
| `BUILD_SHARED_LIBS` | `OFF` | 共有ライブラリをビルドします。 |
| `RUST_BINDINGS` | `ON` | Rust バインディングをビルドします。 |
| `BUILD_PCRE2` | `ON` | PCRE2 のサポートを含めてビルドします。 |
| `BUILD_ADDR2LINE` | `OFF` | addr2line ヘルパーのサポートをビルドします。 |
| `BUILD_WITH_LEVELDB` | `OFF` | LevelDB のサポートを有効にします。 |
| `BUILD_ROCKSDB` | Linux では `OFF`、それ以外では `ON` | RocksDB をソースからビルドします。 |
| `ROCKSDB_USE_DEPS` | Linux では `ON`、それ以外では `OFF` | RocksDB をビルドする代わりに `deps/` のビルド済みバイナリを使用します。 |
| `TD_USE_ROCKSDB` | `ON` | RocksDB のサポートを有効にします。 |
| `BUILD_WITH_LZ4` | `ON` | LZ4 のサポートを含めてビルドします。 |
| `BUILD_S3` | Linux では `ON`、ただしコミュニティ版ビルドでは強制的に `OFF` | S3 関連のビルドパスを有効にします。コミュニティ版ビルドではこれをオフにします。 |
| `BUILD_WITH_S3` | Linux では `ON`、ただしコミュニティ版ビルドでは強制的に `OFF` | 利用可能な場合に S3 のサポートを含めてビルドします。 |
| `BUILD_WITH_COS` | `OFF` | COS のサポートを含めてビルドします。 |
| `BUILD_WITH_LZMA2` | `ON` | LZMA2 のサポートを含めてビルドします。 |
| `BUILD_WITH_ANALYSIS` | Linux では `ON` | 分析関連のビルドパスを有効にします。 |
| `BUILD_WITH_SQLITE` | `OFF` | SQLite のサポートを含めてビルドします。 |
| `BUILD_WITH_BDB` | `OFF` | Berkeley DB のサポートを含めてビルドします。 |
| `BUILD_WITH_LUCENE` | `OFF` | Lucene のサポートを含めてビルドします。 |
| `BUILD_WITH_NURAFT` | `OFF` | NuRaft のサポートを含めてビルドします。 |
| `BUILD_WITH_UV` | `ON` | libuv のサポートを含めてビルドします。 |
| `BUILD_WITH_UV_TRANS` | `ON` | libuv トランスポートのサポートを含めてビルドします。 |
| `BUILD_DEPENDENCY_TESTS` | Linux では `ON` | 依存関係のテストをビルドします。 |
| `BUILD_DOCS` | `OFF` | Doxygen ドキュメントをビルドします。 |
| `BUILD_WITH_INVERTEDINDEX` | `ON` | 転置インデックスのサポートを有効にします。 |
| `BUILD_TAOSD_INTEGRATED` | `OFF` | `taosd` を統合ライブラリとしてビルドします。 |
| `BUILD_AS_LIB` | `OFF` | TDengine をライブラリとしてビルドします。 |
| `BUILD_RELEASE` | `OFF` | リリース版のビルドパスを有効にします。 |
| `BUILD_LIBSASL` | `OFF` | libsasl2 を使用してビルドします。 |
| `BUILD_FLEX_DEPLOY` | `OFF` | 柔軟なデプロイモードを有効にします。 |
| `BUILD_WITH_RAND_ERR` | `OFF` | ランダムなエラー注入を有効にします。 |
| `BUILD_TSZ_ENABLED` | `ON` | TSZ 圧縮のサポートを有効にします。 |
| `BUILD_USE_PUBLIC_DEPS` | `OFF` | 外部依存関係の取得に、代替ミラーではなくインターネット上の公開 URL を使用します。 |

例:

```bash
cmake .. -DBUILD_CONTRIB=ON -DCMAKE_BUILD_TYPE=Release -DBUILD_TOOLS=ON
```

### 4.3 ビルド成果物

`debug/` 以下のデフォルトのビルドツリーは次のようになります:

- バイナリは `debug/build/bin/` に配置されます
  - `taosd` — TDengine サーバーデーモン
  - `taos` — TDengine CLI クライアント
  - `taosBenchmark` — ベンチマークツール（`BUILD_TOOLS=ON`）
  - `taosdump` — インポート/エクスポートツール（`BUILD_TOOLS=ON`）
- ライブラリは `debug/build/lib/` に配置されます
  - Linux では `libtaos.so`（またはプラットフォーム相当のクライアントライブラリ）

## 5. テスト

### 5.1 ユニットテスト

```bash
cd debug
cmake .. -DBUILD_CONTRIB=ON -DBUILD_TEST=ON
make -j$(nproc)
ctest --output-on-failure
```

一部の個別のテストバイナリも `debug/build/bin/` に出力されます。

### 5.2 統合テスト

TDengine には、`tests/` に Python ベースのテストフレームワークが含まれています。

```bash
# 先にサーバーとツールをビルドします
cd debug
cmake .. -DBUILD_CONTRIB=ON -DBUILD_TOOLS=ON -DBUILD_TEST=ON
make -j$(nproc)

# 生成されたテスト用設定で taosd を起動します
./build/bin/taosd -c test/cfg &

# システムテストを実行します
cd ../tests/system-test
python3 test.py -f 2-query/basic.py
```

テストの詳細については、[`tests/README.md`](tests/README.md) を参照してください。

## 6. パッケージング

Linux でのビルドに成功した後、`source/taos-community/packaging/pack_community_tar.sh` を使用してオープンソースのコミュニティ版 tarball を作成します。

このスクリプトは、`taos-community` リポジトリ自体からビルドされた成果物のみをパッケージ化します:

- `taosd`
- `taos`
- `taosBenchmark`（`BUILD_TOOLS=ON` で、存在する場合）
- `taosdump`（`BUILD_TOOLS=ON` で、存在する場合）
- `taosudf`（存在する場合）
- `libtaos.so`
- `libtaosnative.so`
- ヘッダー、`examples/c`、およびインストールスクリプト

`taoskeeper`、`taos-explorer`、`taosx`、コネクター、`taosinspect` のように、他のリポジトリに存在するコンポーネントや、別のビルドフローを必要とするコンポーネントは**パッケージ化しません**。

### 6.1 コミュニティ版 tarball のパッケージング

まずビルドします:

```bash
mkdir debug && cd debug
cmake .. -DCMAKE_BUILD_TYPE=Release -DBUILD_CONTRIB=ON -DBUILD_TOOLS=ON
make -j$(nproc)
```

次に、リポジトリのルートからパッケージングスクリプトを実行します:

```bash
cd ..
./source/taos-community/packaging/pack_community_tar.sh -c debug -n 3.3.6.0
```

オプションの引数:

- `-m <compatible_version>` — デフォルトは `3.0.0.0`
- `-V <stable|beta|preRelease>` — デフォルトは `stable`

例:

```bash
./source/taos-community/packaging/pack_community_tar.sh \
  -c debug \
  -n 3.3.6.0 \
  -m 3.0.0.0 \
  -V stable
```

生成されたパッケージは次の場所に出力されます:

```bash
source/taos-community/release/
```

典型的な出力:

```bash
source/taos-community/release/TDengine-server-3.3.6.0-Linux-x64.tar.gz
source/taos-community/release/TDengine-client-3.3.6.0-Linux-x64.tar.gz
```

サーバーとクライアントのアーカイブは、既存のパッケージングフローと同じ 2 層構造を採用しています:

- 配布用の外側の tarball
- `bin/`、`cfg/`、`inc/` を含む内側の `package.tar.gz`

これはリポジトリ内の OSS 成果物に対する標準的なパッケージング手法であり、今後のリポジトリでも一貫して踏襲してください。

## 7. パッケージからのインストール

`packaging/tools/makepkg.sh` によって生成された公式のインストーラーパッケージの場合:

```bash
tar -xzf TDengine-server-<version>-Linux-x86_64.tar.gz
cd TDengine-server-<version>
sudo ./install.sh
```

パッケージ化されたアーカイブではなく、ローカルのソースビルドから直接インストールする場合は、次のように実行することもできます:

```bash
cd debug
sudo make install
```

## 8. 実行

```bash
# インストール後にサーバーを起動します
sudo systemctl start taosd

# CLI で接続します
taos
```

ビルドツリーからローカルで手早く実行するには、次のようにします:

```bash
cd debug
./build/bin/taosd -c test/cfg
```

続いて、別のシェルで次を実行します:

```bash
cd debug
./build/bin/taos -c test/cfg
```

## 9. ワークフロー

TDengine のビルドチェックのワークフローは、この [GitHub Action](https://github.com/taosdata/TDengine/actions/workflows/taosd-ci-build.yml) で確認できます。今後さらに多くのワークフローが利用可能になる予定です。

## 10. カバレッジ

TDengine の最新のテストカバレッジレポートは [coveralls.io](https://coveralls.io/github/taosdata/TDengine) で確認できます。

<details>

<summary>カバレッジレポートをローカルで実行するには？</summary>
テストカバレッジレポート（HTML 形式）をローカルで作成するには、次のコマンドを実行してください:

```bash
cd tests
bash setup-lcov.sh -v 1.16 && ./run_local_coverage.sh -b main -c task
# main ブランチで longtimeruning_cases.task のケースを実行します
# オプションの詳細については ./run_local_coverage.sh -h を参照してください
```

> **注意:**
> -b および -i オプションを指定すると、TDengine が -DCOVER=true オプション付きで再コンパイルされるため、時間がかかる場合があります。

</details>

## 11. コントリビュート

TDengine へのコントリビュートは、[コントリビューションガイドライン](CONTRIBUTING.md)に従ってください。

## 12. ライセンス

TDengine は [GNU Affero General Public License 3.0](https://github.com/taosdata/TDengine/blob/main/LICENSE) の下でライセンスされています。
