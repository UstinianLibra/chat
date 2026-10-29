# 流式 Socket 多线程中英文聊天室（C++17）

使用 C++17、POSIX 基本 Socket 函数（`socket`/`bind`/`listen`/`accept`/`connect`/`send`/`recv`）
实现的多人在线聊天室。Socket 部分没有使用任何封装类或网络框架；线程与同步使用
`std::thread`、`std::mutex`、`std::condition_variable`、`std::atomic`。

TCP 字节流上使用自定义 **CHAT/1** 长度前缀协议，每条聊天消息都带服务器时间戳与序号，
支持中文、英文和多人并发聊天。

完整实验报告（协议语法/语义/时序、模块划分、流程图、使用方法、测试与丢包观察）见
[`docs/report.md`](docs/report.md)。

## 功能

- 流式 Socket（TCP），C/S 架构，一个服务器支持多人同时在线。
- 自定义应用层协议：`长度前缀 + UTF-8 消息体`，正确处理粘包/半包。
- 聊天消息由服务器加盖毫秒级时间戳（`YYYY-MM-DD HH:MM:SS.mmm`）。
- 中文/英文均为 UTF-8 原样传输，不做转码。
- 服务端：主线程 `accept`，每个客户端一个 `std::thread`，广播式聊天。
- 客户端：命令行输入循环 + 独立接收线程，异步显示消息。
- 登录握手、昵称唯一性校验、在线列表 `/who`、PING 时延、退出通知。
- 应用层序号 + `/stats`，可观察消息丢失/重复。

## 编译

要求：macOS 或 Linux，支持 C++17 的编译器（`c++`/`clang++`/`g++`）、`make`、
POSIX 线程库。

```bash
make
```

生成：

```text
build/chat_server
build/chat_client
```

清理：

```bash
make clean
```

## 运行

终端 1，启动服务器（端口可省略，默认 9000）：

```bash
./build/chat_server 9000
```

终端 2/3/4，启动多个客户端：

```bash
./build/chat_client 127.0.0.1 9000 Ann
./build/chat_client 127.0.0.1 9000 李明
./build/chat_client 127.0.0.1 9000 haruki
```

客户端参数：

```text
chat_client <server-ip> <port> <nickname>
```

昵称是 UTF-8 字符串，最多 32 字节，不能包含控制字符，不能以空格开头或结尾。

连接成功后直接输入文字并回车即可发送，例如：

```text
大家好，Hello world!
你好，今天过得怎么样？
```

## 客户端命令

| 命令 | 作用 |
|---|---|
| `/help` | 显示帮助 |
| `/who` | 查看在线用户 |
| `/stats` | 显示已发送、已接收、缺失、重复/乱序统计 |
| `/ping` | 测量到服务器的往返时延 RTT |
| `/quit` 或 `/exit` | 离开聊天室 |

## 自动测试

```bash
make test
```

启动 4 个 TCP 客户端，每个发送 200 条中文/英文混合消息，验证每个客户端都收到
全部 800 条广播消息、序号连续、时间戳存在、UTF-8 往返正确。

也可对已运行的服务器测试：

```bash
python3 tools/load_test.py --host 127.0.0.1 --port 9000 \
    --clients 8 --messages 500
```

该脚本只用于测试，聊天程序本身完全由 C++ 实现。

## 目录结构

```text
.
├── Makefile
├── README.md
├── src/
│   ├── protocol.h      # CHAT/1 协议定义与接口
│   ├── protocol.cpp    # 组帧/拆帧、严格解析、时间格式化
│   ├── server.cpp      # 监听、每客户端线程、在线表、广播
│   └── client.cpp      # 连接、握手、接收线程、命令行与统计
├── tools/
│   └── load_test.py    # 应用层消息完整性测试工具
└── docs/
    └── report.md       # 完整实验报告（协议/设计/流程图/测试）
```

## 说明

- Socket 全部使用原始 POSIX 调用，未使用封装后的 Socket 类或网络框架。
- TCP 是可靠、有序的字节流；网络层丢包由 TCP 重传恢复，应用层消息不应丢失。
  程序中的序号与 `/stats` 用于验证这一点。
- 服务器把在线表锁同时用作发送串行化锁，保证同一客户端的帧不会交错；服务器
  设置 5 秒发送超时，避免慢客户端无限阻塞广播。
- 当前版本明文传输、无账号认证、无历史消息、单服务器广播。
