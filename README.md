<div align="center">

# 🔌 MaiBot Napcat 适配器（第三方改版）

<p>
  <strong>🚀 高性能的 MaiBot 与 Napcat 适配器，提供增强的功能和优化体验</strong>
</p>

> [!IMPORTANT]
> **声明**: 本项目为第三方改版，并非官方原版项目，请注意区分

<div align="center">

[![GitHub](https://img.shields.io/badge/GitHub-NapcatAdapter-ff6b35?logo=github&logoColor=white&style=for-the-badge)](https://github.com/MaiBot-Plus/Napcat-Adapter)
[![License](https://img.shields.io/badge/License-GPLv3-d73a49?logo=gnu&logoColor=white&style=for-the-badge)](LICENSE)
[![Python](https://img.shields.io/badge/Python-3.8+-3776ab?logo=python&logoColor=white&style=for-the-badge)](https://python.org)
[![WebSocket](https://img.shields.io/badge/WebSocket-Supported-brightgreen?logo=websocket&logoColor=white&style=for-the-badge)](https://websockets.readthedocs.io/)
[![QQ](https://img.shields.io/badge/QQ-Bot-blue?style=for-the-badge&logo=tencentqq&logoColor=white)](https://github.com/NapNeko/NapCatQQ)

</div>

</div>

---

## 📖 项目简介

本项目是 **MaiBot 与 Napcat 适配器** 的第三方修改版本，专注于提供更稳定、更高效的消息转发和处理服务。通过优化的架构设计和丰富的功能特性，为用户带来更好的使用体验。

---

## ✨ 项目特性

<table>
<tr>
<td width="50%">

### 🔄 核心功能

- 💬 **多消息类型支持** - 文本、图片、语音、视频等全类型消息
- 🔄 **完整消息转发** - 高效的消息转发和处理机制  
- 🛡️ **连接管理** - 内置心跳检测和自动重连
- 🎯 **消息队列** - 优雅的消息队列处理系统
- 🔧 **管理员功能** - 丰富的机器人管理功能
- 🔄 **自动重连** - 断线自动重连，保障服务稳定性

</td>
<td width="50%">

### 🚀 增强特性

- 🌐 **双向 WebSocket** - 支持正向和反向连接模式
- 📡 **分片传输** - WebSocket 分片传输支持大数据包
- 📋 **消息聚合** - 智能消息聚合系统，优化理解效果
- 🔐 **身份验证** - 支持访问令牌身份验证

</td>
</tr>
</table>

---

## � 使用说明

### 📖 文档链接

如需了解详细的配置和使用方法，请参考官方文档：

🔗 **[官方使用指南](https://docs.mai-mai.org/manual/adapters/napcat.html)**

---

## �️ 安装部署

### 📦 环境准备

```bash
# 克隆项目
git clone https://github.com/MaiBot-Plus/Napcat-Adapter.git
cd Napcat-Adapter

# 安装依赖
pip install -r requirements.txt

# 复制配置文件
cp config/config.toml.example config/config.toml
```

### ⚙️ WebSocket 连接模式

本适配器支持两种灵活的 WebSocket 连接模式，满足不同的部署需求：

<table>
<tr>
<td width="50%">

#### 🔄 反向连接模式（默认推荐）

```toml
[napcat_server]
mode = "reverse"  # 反向连接模式
host = "0.0.0.0"
port = 8095
```

**特点：**
- 🔧 适配器作为 WebSocket 服务器
- 📞 Napcat 客户端主动连接
- 🌐 默认监听 `ws://0.0.0.0:8095`
- 🛡️ 更好的网络安全控制

</td>
<td width="50%">

#### ➡️ 正向连接模式（新增功能）

```toml
[napcat_server]
mode = "forward"  # 正向连接模式
url = "ws://localhost:3001"
access_token = "your_token_here"  # 可选
```

**特点：**
- 📱 适配器作为 WebSocket 客户端
- 🚀 适配器主动连接到 Napcat
- 🔄 支持自动重连机制
- 🔐 支持访问令牌身份验证

</td>
</tr>
</table>

### 📋 配置参数详解

| 参数 | 类型 | 必填 | 适用模式 | 说明 |
|------|------|------|----------|------|
| `mode` | `string` | ✅ | 通用 | 连接模式：`"reverse"` 或 `"forward"` |
| `host` | `string` | ✅ | 反向模式 | 监听主机地址（如：`"0.0.0.0"`） |
| `port` | `int` | ✅ | 反向模式 | 监听端口（如：`8095`） |
| `url` | `string` | ✅ | 正向模式 | Napcat WebSocket 服务器地址 |
| `access_token` | `string` | ❌ | 通用 | WebSocket 连接的访问令牌 |

---

## 📊 消息流转架构

<div align="center">

```mermaid
graph TB
    subgraph "🔌 连接层"
        NC[🐱 Napcat客户端]
        WS1[WebSocket连接]
        AD[🔌 MaiBot-Napcat适配器]
        WS2[WebSocket连接]
        MB[🤖 MaiBot服务]
    end
    
    subgraph "📨 消息处理层"
        MQ[📦 消息队列]
        MH[⚙️ 消息处理器]
        MP[🔄 消息解析器]
    end
    
    subgraph "🛡️ 管理层"
        HB[💓 心跳检测]
        CM[🔧 连接管理]
        CFG[⚙️ 配置管理]
    end
    
    NC -->|消息发送| WS1
    WS1 --> AD
    AD --> MQ
    MQ --> MH
    MH --> MP
    MP -->|解析后消息| WS2
    WS2 --> MB
    
    HB -.->|监控| WS1
    HB -.->|监控| WS2
    CM -.->|管理| AD
    CFG -.->|配置| AD
    
    style NC fill:#e1f5fe
    style AD fill:#f3e5f5
    style MB fill:#e8f5e8
    style MQ fill:#fff3e0
    style MH fill:#fce4ec
```

</div>

### 🔄 详细流程说明

<details>
<summary><strong>📋 点击展开查看详细的消息处理流程</strong></summary>

```mermaid
sequenceDiagram
    participant NC as 🐱 Napcat客户端
    participant AD as 🔌 适配器
    participant MQ as 📦 消息队列
    participant MH as ⚙️ 消息处理器
    participant MB as 🤖 MaiBot服务

    Note over NC,MB: 🚀 初始化阶段
    NC->>AD: WebSocket连接(ws://localhost:8095)
    AD->>MB: WebSocket连接(ws://localhost:8000)
    
    Note over NC,MB: 💓 心跳检测
    loop 每30秒
        NC->>AD: 发送心跳包
        AD->>NC: 心跳响应
    end

    Note over NC,MB: 📨 消息处理流程
    NC->>AD: 📤 发送消息
    AD->>MQ: 📦 消息入队(message_queue)
    MQ->>MH: 📥 消息出队处理
    MH->>MH: 🔍 解析消息类型
    
    alt 📝 文本消息
        MH->>MB: 📝 发送文本消息
    else 🖼️ 图片消息
        MH->>MB: 🖼️ 发送图片消息
    else 🎭 混合消息
        MH->>MB: 🎭 发送混合消息
    else 📋 转发消息
        MH->>MB: 📋 发送转发消息
    end
    
    MB-->>AD: ✅ 消息响应
    AD-->>NC: ✅ 消息响应

    Note over NC,MB: 🔚 优雅关闭
    AD->>MB: 🔌 关闭连接
    AD->>MQ: 🧹 清空消息队列
    AD->>NC: 🔌 关闭连接
```

</details>

---


## 关键组件

<div align="center">

| 组件 | 图标 | 功能描述 | 技术特点 |
|------|------|----------|----------|
| **消息处理器** | 📨 | 负责解析和转发各类消息 | 支持多种消息格式，智能路由 |
| **连接管理器** | 🔌 | 维护 WebSocket 连接状态 | 自动重连、心跳检测、状态监控 |
| **消息队列** | 📦 | 保障消息处理顺序性 | 异步处理、防丢失、流量控制 |
| **配置管理** | ⚙️ | 灵活的配置系统支持 | 热重载、多环境、参数验证 |

</div>

---

## 🚀 快速开始

### 1️⃣ 启动适配器

```bash
# 启动适配器服务
python main.py

# 或使用后台运行
nohup python main.py > logs/adapter.log 2>&1 &
```

### 2️⃣ 配置验证

启动后，适配器会自动：
- ✅ 验证配置文件完整性
- 🔌 建立 WebSocket 连接
- 💓 开始心跳检测
- 📦 初始化消息队列

### 3️⃣ 状态监控

查看运行状态：
```bash
# 查看日志
tail -f logs/adapter.log

# 检查进程
ps aux | grep main.py
```

---


## ⚠️ 免责声明

> [!CAUTION]
> **重要提醒**
> 
> - 本项目为 **第三方修改版本**，与原始项目和官方团队无关
> - 使用本项目产生的任何问题，请在 **本仓库** 提交 Issue
> - **请勿** 将相关问题反馈至原项目维护者
> - 本项目仅供学习交流使用，请遵守相关法律法规

---

## 📄 开源协议

<div align="center">

**本项目基于 [GPL-3.0](LICENSE) 协议开源**

[![GPL-3.0](https://img.shields.io/badge/License-GPL--3.0-blue.svg?style=for-the-badge&logo=gnu)](LICENSE)

```
Copyright © 2024 MaiBot-Plus Community
Licensed under the GNU General Public License v3.0
```

</div>

---

<div align="center">

## 🌟 支持项目

**如果这个项目对你有帮助，请给我们一个 Star！⭐**

**💬 遇到问题？欢迎提交 Issue 或 PR！**

**🔗 相关项目：[墨狐Bot](https://github.com/MoFox-Studio/MoFox_Bot)**

---

> **⚠️ 再次提醒：本项目为第三方改版，请勿将问题反馈至原项目**

Made with ❤️ by [MoFox-Studio](https://github.com/MoFox-Studio/Napcat-Adapter)

</div>