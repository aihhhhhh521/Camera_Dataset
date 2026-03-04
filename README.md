# Camera_Dataset

## Wikicommons 抓取配置建议

可按目标在 `scripts/config.yaml` 的 `wikicommons` 与 `pipeline_filter` 相关字段切换两种策略：

### 1) 质量优先（推荐用于高质量重建）
- `stop_when_exhausted: true`
- `allow_cross_keyword_backfill: false`
- `backfill_max_keywords: 0`（关闭 backfill）
- `backfill_max_pages_per_keyword: 0`
- 保持较严格过滤阈值（`pipeline_filter`）

特点：更少长尾样本，更稳定的清晰度/构图质量，但可能达不到目标数量。

### 2) 数量优先（默认）
- `stop_when_exhausted: false`
- `allow_cross_keyword_backfill: true`
- `backfill_max_keywords: 6`
- `backfill_max_pages_per_keyword: 8`
- 必要时放宽 `pipeline_filter` 阈值

特点：更容易凑满 `target_count`，但更依赖后续清洗，长尾样本质量波动更大。

> 脚本会在日志中输出提前结束原因（如：关键词耗尽、429 过高、过滤过严、达到 target），便于诊断是“源不足”还是“阈值过紧”。

## 每张图片 JSON 元数据字段说明

每张图片会在 `metadata/<image_id>.json` 生成对应元数据文件。`wikicommons_scraper.py`、`openverse_scraper.py`、`flickr_scraper.py` 都使用同一套核心字段结构（来源不同，字段值会不同）。

### 顶层字段

| 字段名 | 类型 | 含义 |
|---|---|---|
| `image_id` | `string` | 图片唯一 ID，例如 `WCM_00001`、`OPV_00001`、`FLK_00001`。 |
| `category` | `string` | 业务分类（如 `portraits`、`natural_landscapes`）。 |
| `source_platform` | `string` | 数据来源平台名称（Wikimedia Commons / Openverse / Flickr）。 |
| `original_url` | `string` | 原始图片下载 URL。 |
| `source_page_url` | `string` | 图片原网页/详情页 URL（用于回溯和版权核验）。 |
| `author` | `string` | 作者/上传者名称（可能为空）。 |
| `license_type` | `string` | 许可类型（例如 CC BY、CC BY-SA、CC0、Public Domain）。 |
| `license_url` | `string` | 许可条款链接（若来源未提供可能为空）。 |
| `search_keyword` | `string` | 当前图片命中的抓取关键词。 |
| `resolution` | `number[2]` | 图像分辨率，格式固定为 `[W, H]`。 |
| `exif_data` | `object` | 从 EXIF 提取的相机参数（可能为空值）。 |
| `pipeline_metrics` | `object` | 质量过滤流水线计算出的指标值。 |

### `exif_data` 子字段

| 字段名 | 类型 | 含义 |
|---|---|---|
| `focal_length` | `string/null` | 焦距，不存在则为 `null`。 |
| `aperture` | `string/null` | 光圈值，不存在则为 `null`。 |
| `exposure_time` | `string/null` | 快门时间，不存在则为 `null`。 |
| `iso` | `string/null` | ISO 感光度，缺失时为 `null`。 |

### `pipeline_metrics` 常见子字段

> 实际字段会随过滤配置启用项变化（例如是否启用 saliency/depth），下面是常见键：

| 字段名 | 类型 | 含义 |
|---|---|---|
| `filesize_kb` | `number` | 图片文件大小（KB）。 |
| `header_W` / `header_H` | `number` | 从图像头预读到的宽高（用于快速过滤超大图）。 |
| `header_pixels` | `number` | 预读宽高对应的总像素数。 |
| `W` / `H` | `number` | 实际解码后的宽高。 |
| `min_side` | `number` | 短边长度。 |
| `aspect_ratio` | `number` | 长宽比（较大边 / 较小边）。 |
| `laplacian_var` | `number` | 清晰度指标（拉普拉斯方差）。 |
| `subject_saliency_ratio` | `number` | 主体显著区域占比（开启 saliency 时存在）。 |
| `depth_grad_var` | `number` | 深度梯度方差（开启深度估计时存在）。 |

### 说明

- 同一字段在不同来源中语义一致，便于后续统一清洗/训练。
- 某些字段为空是正常现象（例如来源站点未提供 EXIF、作者或 license_url）。
- `pipeline_metrics` 是后续质量分析的重要依据，建议保留原始数值不要手工改写。