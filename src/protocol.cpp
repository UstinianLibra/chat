#include "protocol.h"

#include <array>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <sys/socket.h>
#include <unistd.h>

namespace chat {
namespace {

constexpr std::array<std::string_view, 12> kTypeNames = {
    "HELLO", "WELCOME", "MSG",    "JOIN", "LEAVE", "SYS",
    "LIST",  "LIST_REPLY", "PING", "PONG", "QUIT", "ERROR",
};

enum class IoStatus { Ok, Closed, Error };

int send_all(int fd, const char *data, std::size_t len)
{
    std::size_t sent = 0;

    while (sent < len) {
#ifdef MSG_NOSIGNAL
        const ssize_t n = send(fd, data + sent, len - sent, MSG_NOSIGNAL);
#else
        const ssize_t n = send(fd, data + sent, len - sent, 0);
#endif
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        if (n == 0) {
            errno = EPIPE;
            return -1;
        }
        sent += static_cast<std::size_t>(n);
    }
    return 0;
}

IoStatus recv_all(int fd, char *data, std::size_t len)
{
    std::size_t received = 0;

    while (received < len) {
        const ssize_t n = recv(fd, data + received, len - received, 0);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return IoStatus::Error;
        }
        if (n == 0) {
            return IoStatus::Closed;
        }
        received += static_cast<std::size_t>(n);
    }
    return IoStatus::Ok;
}

RecvStatus read_header_line(int fd, std::string &line, std::string &error)
{
    line.clear();

    for (;;) {
        char ch = '\0';
        const ssize_t n = recv(fd, &ch, 1, 0);

        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            error = std::string("recv header: ") + std::strerror(errno);
            return RecvStatus::Error;
        }
        if (n == 0) {
            if (line.empty()) {
                return RecvStatus::Closed;
            }
            error = "connection closed before end of header";
            return RecvStatus::ProtocolError;
        }
        if (ch == '\n') {
            break;
        }
        if (line.size() + 1 >= kMaxHeader) {
            error = "header exceeds " + std::to_string(kMaxHeader - 1) +
                    " bytes";
            return RecvStatus::ProtocolError;
        }
        line.push_back(ch);
    }

    while (!line.empty() && line.back() == '\r') {
        line.pop_back();
    }
    if (line.empty()) {
        error = "empty header";
        return RecvStatus::ProtocolError;
    }
    return RecvStatus::Ok;
}

bool parse_u64(std::string_view text, std::uint64_t &value)
{
    if (text.empty()) {
        return false;
    }

    std::uint64_t result = 0;
    for (const char ch : text) {
        if (ch < '0' || ch > '9') {
            return false;
        }
        const auto digit = static_cast<std::uint64_t>(ch - '0');
        if (result > (UINT64_MAX - digit) / 10ULL) {
            return false;
        }
        result = result * 10ULL + digit;
    }
    value = result;
    return true;
}

bool split_header(std::string_view line,
                  std::array<std::string_view, 6> &fields,
                  std::string &error)
{
    std::size_t start = 0;

    for (std::size_t i = 0; i < 5; ++i) {
        const std::size_t tab = line.find('\t', start);
        if (tab == std::string_view::npos || tab == start) {
            error = "header must contain exactly 6 non-empty tab-separated fields";
            return false;
        }
        fields[i] = line.substr(start, tab - start);
        start = tab + 1;
    }
    if (start >= line.size() || line.find('\t', start) != std::string_view::npos) {
        error = "header must contain exactly 6 non-empty tab-separated fields";
        return false;
    }
    fields[5] = line.substr(start);
    return true;
}

} // 匿名命名空间

std::uint64_t now_ms()
{
    struct timespec ts {};

    if (clock_gettime(CLOCK_REALTIME, &ts) != 0) {
        return static_cast<std::uint64_t>(std::time(nullptr)) * 1000ULL;
    }
    return static_cast<std::uint64_t>(ts.tv_sec) * 1000ULL +
           static_cast<std::uint64_t>(ts.tv_nsec / 1000000L);
}

std::string format_time(std::uint64_t timestamp_ms)
{
    const std::time_t seconds =
        static_cast<std::time_t>(timestamp_ms / 1000ULL);
    const unsigned int millis =
        static_cast<unsigned int>(timestamp_ms % 1000ULL);
    std::tm local {};
    char date[32];
    char output[40];

    if (localtime_r(&seconds, &local) == nullptr ||
        std::strftime(date, sizeof(date), "%Y-%m-%d %H:%M:%S", &local) == 0) {
        return "unknown-time";
    }
    std::snprintf(output, sizeof(output), "%s.%03u", date, millis);
    return output;
}

std::string_view type_name(Type type)
{
    const auto index = static_cast<std::size_t>(type);
    if (index >= kTypeNames.size()) {
        return "UNKNOWN";
    }
    return kTypeNames[index];
}

bool parse_type(std::string_view name, Type &type)
{
    for (std::size_t i = 0; i < kTypeNames.size(); ++i) {
        if (name == kTypeNames[i]) {
            type = static_cast<Type>(i);
            return true;
        }
    }
    return false;
}

bool validate_nick(std::string_view nick, std::string &reason)
{
    reason.clear();

    if (nick.empty()) {
        reason = "nickname must not be empty";
        return false;
    }
    if (nick.size() > kMaxNick) {
        reason = "nickname is too long (max " + std::to_string(kMaxNick) +
                 " UTF-8 bytes)";
        return false;
    }
    if (nick.front() == ' ' || nick.back() == ' ') {
        reason = "nickname must not begin or end with a space";
        return false;
    }
    for (const char ch : nick) {
        const auto c = static_cast<unsigned char>(ch);
        /* 大于等于 0x80 的字节属于多字节 UTF-8 字符。 */
        if (c < 0x20 || c == 0x7f) {
            reason = "nickname contains a control character";
            return false;
        }
    }
    return true;
}

int send_frame(int fd, Type type, std::uint64_t seq, std::string_view sender,
               std::uint64_t timestamp_ms, std::string_view body)
{
    if (sender.empty() || sender.size() > kMaxNick ||
        sender.find_first_of("\t\r\n") != std::string_view::npos) {
        errno = EINVAL;
        return -1;
    }
    if (body.size() > kMaxBody) {
        errno = EMSGSIZE;
        return -1;
    }

    std::string header;
    header.reserve(128);
    header.append(kProtocol);
    header.push_back('\t');
    header.append(type_name(type));
    header.push_back('\t');
    header.append(std::to_string(seq));
    header.push_back('\t');
    header.append(sender);
    header.push_back('\t');
    header.append(std::to_string(timestamp_ms));
    header.push_back('\t');
    header.append(std::to_string(body.size()));
    header.append("\r\n");

    if (header.size() >= kMaxHeader) {
        errno = EMSGSIZE;
        return -1;
    }
    if (send_all(fd, header.data(), header.size()) != 0) {
        return -1;
    }
    if (!body.empty() && send_all(fd, body.data(), body.size()) != 0) {
        return -1;
    }
    return 0;
}

RecvStatus recv_frame(int fd, Frame &frame, std::string &error)
{
    std::string line;
    std::array<std::string_view, 6> fields {};
    std::uint64_t seq = 0;
    std::uint64_t timestamp = 0;
    std::uint64_t body_len = 0;
    Type type = Type::Error;

    frame = Frame {};
    error.clear();

    const RecvStatus line_status = read_header_line(fd, line, error);
    if (line_status != RecvStatus::Ok) {
        return line_status;
    }
    if (!split_header(line, fields, error)) {
        return RecvStatus::ProtocolError;
    }
    if (fields[0] != kProtocol) {
        error = "unsupported protocol '" + std::string(fields[0]) + "'";
        return RecvStatus::ProtocolError;
    }
    if (!parse_type(fields[1], type)) {
        error = "unknown message type '" + std::string(fields[1]) + "'";
        return RecvStatus::ProtocolError;
    }
    if (!parse_u64(fields[2], seq)) {
        error = "invalid sequence number '" + std::string(fields[2]) + "'";
        return RecvStatus::ProtocolError;
    }
    if (fields[3].size() > kMaxNick) {
        error = "sender field is too long";
        return RecvStatus::ProtocolError;
    }
    if (!parse_u64(fields[4], timestamp)) {
        error = "invalid timestamp '" + std::string(fields[4]) + "'";
        return RecvStatus::ProtocolError;
    }
    if (!parse_u64(fields[5], body_len) || body_len > kMaxBody) {
        error = "invalid body length '" + std::string(fields[5]) + "'";
        return RecvStatus::ProtocolError;
    }

    frame.type = type;
    frame.seq = seq;
    frame.sender.assign(fields[3]);
    frame.timestamp_ms = timestamp;
    frame.body.resize(static_cast<std::size_t>(body_len));

    if (body_len > 0) {
        const IoStatus io = recv_all(fd, frame.body.data(),
                                     static_cast<std::size_t>(body_len));
        if (io == IoStatus::Closed) {
            error = "connection closed inside message body";
            frame = Frame {};
            return RecvStatus::ProtocolError;
        }
        if (io == IoStatus::Error) {
            error = std::string("recv body: ") + std::strerror(errno);
            frame = Frame {};
            return RecvStatus::Error;
        }
    }
    return RecvStatus::Ok;
}

} // 命名空间 chat
