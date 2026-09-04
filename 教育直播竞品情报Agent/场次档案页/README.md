# 直播场次档案页

本地只读场次档案服务。服务从 Runtime V3 的 PostgreSQL 读取场次、录制、转录和分析信息；原始媒体只展示指定录制根目录内的真实路径，不提供任意文件访问。

## 启动

```bash
/Users/mac/.local/share/edu-live-runtime-v3-venv/bin/python server.py
```

默认地址：`http://127.0.0.1:8765/`

## 设计边界

- PostgreSQL 是唯一事实源；页面不写数据库。
- 逐字稿按页加载，每页100段，支持关键词检索。
- 仅允许读取 `/Volumes/ExternalStorage/同行直播录制` 下的路径。
- 需要长期访问或多人访问时，应在明确权限后再部署到妙搭/云端，并为媒体配置独立存储。
