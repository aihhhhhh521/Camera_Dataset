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