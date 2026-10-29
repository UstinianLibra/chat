#ifndef CHAT_PROTOCOL_H
#define CHAT_PROTOCOL_H

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

/*
 * CHAT/1 应用层协议
 * -----------------
 * 每个帧的格式为：
 *
 *   CHAT/1<TAB>TYPE<TAB>SEQ<TAB>SENDER<TAB>TIMESTAMP_MS<TAB>BODY_LEN<CRLF>
 *   <BODY_LEN 字节的 UTF-8 文本>
 *
 * BODY_LEN 是字节数而不是字符数，因此中文正文也能被正确分帧。
 * 协议层只负责帧的编码与解码，不包含任何 Socket 封装类；
 * 所有 I/O 均使用原始 POSIX Socket API。
 */

namespace chat {

inline constexpr std::string_view kProtocol = "CHAT/1";
inline constexpr int kDefaultPort = 9000;
inline constexpr std::size_t kMaxHeader = 512;
inline constexpr std::size_t kMaxBody = 65536;
inline constexpr std::size_t kMaxMessage = 8192; /* 单条聊天消息 */
inline constexpr std::size_t kMaxNick = 32;      /* UTF-8 字节数 */

enum class Type {
    Hello = 0,
    Welcome,
    Msg,
    Join,
    Leave,
    Sys,
    List,
    ListReply,
    Ping,
    Pong,
    Quit,
    Error,
};

struct Frame {
    Type type = Type::Error;
    std::uint64_t seq = 0;
    std::string sender;
    std::uint64_t timestamp_ms = 0;
    std::string body;
};

enum class RecvStatus {
    Ok,
    Closed,
    Error,
    ProtocolError,
};

std::uint64_t now_ms();

/* 将 Unix 毫秒时间格式化为本地时间 "YYYY-MM-DD HH:MM:SS.mmm"。 */
std::string format_time(std::uint64_t timestamp_ms);

std::string_view type_name(Type type);
bool parse_type(std::string_view name, Type& type);

/* 校验昵称；失败时把可读原因写入 reason。 */
bool validate_nick(std::string_view nick, std::string& reason);

/*
 * 发送一个完整帧。成功返回 0，失败返回 -1 并设置 errno。
 * 若多个线程向同一个 fd 写数据，调用方必须串行化发送。
 */
int send_frame(int fd, Type type, std::uint64_t seq, std::string_view sender,
               std::uint64_t timestamp_ms, std::string_view body);

/*
 * 接收恰好一个完整帧。返回 RecvStatus::Ok 时 frame 已被完整填充；
 * 失败时 error 中包含诊断信息。
 */
RecvStatus recv_frame(int fd, Frame& frame, std::string& error);

} // 命名空间 chat

#endif /* 协议头文件结束 */
