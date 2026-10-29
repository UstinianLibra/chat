#include "protocol.h"

#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <cstring>
#include <exception>
#include <iostream>
#include <mutex>
#include <netdb.h>
#include <netinet/in.h>
#include <sstream>
#include <string>
#include <string_view>
#include <sys/select.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <unordered_map>

namespace {

int g_socket_fd = -1;
std::string g_nick;
std::uint64_t g_next_seq = 1;

std::atomic<bool> g_running {true};
std::atomic<int> g_handshake_state {0}; /* 0 = 等待中，1 = 成功，-1 = 失败 */

std::mutex g_state_mutex;
std::condition_variable g_state_cond;
std::mutex g_output_mutex;
std::mutex g_stats_mutex;
std::mutex g_ping_mutex;

struct ChatStats {
    std::uint64_t sent = 0;
    std::uint64_t received = 0;
    std::uint64_t missing = 0;
    std::uint64_t duplicates = 0;
};

ChatStats g_stats;
std::unordered_map<std::string, std::uint64_t> g_last_seq;
std::uint64_t g_ping_started_ms = 0;

template <typename... Args>
void print_line(const Args &...args)
{
    std::ostringstream line;
    (line << ... << args);

    std::lock_guard<std::mutex> lock(g_output_mutex);
    std::cout << line.str() << '\n' << std::flush;
}

void mark_handshake(int state)
{
    std::lock_guard<std::mutex> lock(g_state_mutex);
    if (g_handshake_state.load() == 0) {
        g_handshake_state.store(state);
    }
    g_state_cond.notify_all();
}

void request_stop()
{
    g_running.store(false);
    if (g_socket_fd >= 0) {
        shutdown(g_socket_fd, SHUT_RDWR);
    }
}

void reset_tracker(const std::string &nick)
{
    std::lock_guard<std::mutex> lock(g_stats_mutex);
    g_last_seq[nick] = 0;
}

void remove_tracker(const std::string &nick)
{
    std::lock_guard<std::mutex> lock(g_stats_mutex);
    g_last_seq.erase(nick);
}

/*
 * 应用层消息丢失检测。SEQ 由发送方分配，并由服务器原样回显。
 * 出现序号缺口说明某条完整聊天消息没有到达当前客户端，
 * 而 TCP 本身不应发生这种情况。
 */
void update_sequence_stats(const std::string &sender, std::uint64_t seq)
{
    if (seq == 0) {
        return;
    }

    std::lock_guard<std::mutex> lock(g_stats_mutex);
    ++g_stats.received;

    const auto it = g_last_seq.find(sender);
    if (it == g_last_seq.end()) {
        g_last_seq.emplace(sender, seq); /* 加入后建立序号基线 */
        return;
    }

    const std::uint64_t last = it->second;
    if (seq > last + 1) {
        g_stats.missing += seq - last - 1;
    } else if (seq <= last) {
        ++g_stats.duplicates;
    }
    if (seq > last) {
        it->second = seq;
    }
}

void print_stats()
{
    ChatStats stats;
    {
        std::lock_guard<std::mutex> lock(g_stats_mutex);
        stats = g_stats;
    }

    print_line("--- 统计 / statistics ---");
    print_line("已发送 / sent:              ", stats.sent);
    print_line("已接收 / received:          ", stats.received);
    print_line("缺失 / missing:             ", stats.missing);
    print_line("重复或乱序 / duplicate:     ", stats.duplicates);
}

void handle_server_frame(const chat::Frame &frame)
{
    const std::string time = chat::format_time(frame.timestamp_ms);

    switch (frame.type) {
    case chat::Type::Welcome:
        print_line('[', time, "] *** ", frame.body);
        mark_handshake(1);
        break;

    case chat::Type::Msg:
        update_sequence_stats(frame.sender, frame.seq);
        print_line('[', time, "] ", frame.sender, ": ", frame.body);
        break;

    case chat::Type::Join:
        reset_tracker(frame.sender);
        print_line('[', time, "] *** ", frame.body);
        break;

    case chat::Type::Leave:
        remove_tracker(frame.sender);
        print_line('[', time, "] *** ", frame.body);
        break;

    case chat::Type::Sys:
        print_line('[', time, "] *** ", frame.body);
        break;

    case chat::Type::ListReply:
        print_line('[', time, "] *** 在线用户 / online: ", frame.body);
        break;

    case chat::Type::Pong:
        {
            std::uint64_t started = 0;
            {
                std::lock_guard<std::mutex> lock(g_ping_mutex);
                started = g_ping_started_ms;
                g_ping_started_ms = 0;
            }
            if (started != 0) {
                const std::uint64_t now = chat::now_ms();
                const std::uint64_t rtt = now >= started ? now - started : 0;
                print_line('[', time, "] *** PONG / 往返时延 RTT: ",
                           rtt, " ms");
            } else {
                print_line('[', time, "] *** PONG");
            }
        }
        break;

    case chat::Type::Error:
        print_line('[', time, "] [错误/error] ", frame.body);
        if (g_handshake_state.load() == 0) {
            mark_handshake(-1);
        }
        break;

    default:
        print_line('[', time, "] *** 忽略未知/不适用消息 ",
                   chat::type_name(frame.type), " / ignored ",
                   chat::type_name(frame.type));
        break;
    }
}

void receiver_thread()
{
    while (g_running.load()) {
        chat::Frame frame;
        std::string error;
        const chat::RecvStatus status =
            chat::recv_frame(g_socket_fd, frame, error);

        if (status == chat::RecvStatus::Ok) {
            handle_server_frame(frame);
            continue;
        }
        if (!g_running.load()) {
            break;
        }
        if (status == chat::RecvStatus::Closed) {
            print_line("*** 与服务器的连接已关闭 / connection closed");
        } else if (status == chat::RecvStatus::ProtocolError) {
            print_line("*** 协议错误 / protocol error: ", error);
        } else {
            print_line("*** 接收错误 / receive error: ", error);
        }
        break;
    }

    mark_handshake(-1);
    g_running.store(false);
}

bool send_frame(chat::Type type, std::uint64_t seq, std::string_view body = {})
{
    if (chat::send_frame(g_socket_fd, type, seq, g_nick, chat::now_ms(),
                         body) != 0) {
        print_line("*** 发送失败 / send failed: ", std::strerror(errno));
        return false;
    }
    return true;
}

int connect_to_server(const std::string &host, const std::string &port)
{
    addrinfo hints {};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    addrinfo *results = nullptr;
    const int gai_result =
        getaddrinfo(host.c_str(), port.c_str(), &hints, &results);
    if (gai_result != 0) {
        print_line("*** 地址解析失败 / getaddrinfo: ",
                   gai_strerror(gai_result));
        return -1;
    }

    int fd = -1;
    int saved_errno = 0;
    for (addrinfo *it = results; it != nullptr; it = it->ai_next) {
        fd = socket(it->ai_family, it->ai_socktype, it->ai_protocol);
        if (fd < 0) {
            saved_errno = errno;
            continue;
        }
        if (connect(fd, it->ai_addr, it->ai_addrlen) == 0) {
            break;
        }
        saved_errno = errno;
        close(fd);
        fd = -1;
    }
    freeaddrinfo(results);

    if (fd < 0) {
        print_line("*** 连接失败 / connect failed: ",
                   std::strerror(saved_errno));
    }
    return fd;
}

void print_help()
{
    print_line("--- 命令 / commands ---");
    print_line("  直接输入文字后回车: 发送聊天消息 / type text and Enter: send");
    print_line("  /who     查看在线用户 / list online users");
    print_line("  /stats   查看消息序号与缺失统计 / loss statistics");
    print_line("  /ping    测量到服务器的往返时延 / measure RTT");
    print_line("  /help    显示本帮助 / show this help");
    print_line("  /quit    离开聊天室 / leave (also /exit)");
}

/* 返回 false 表示输入循环应停止。 */
bool handle_command(const std::string &line)
{
    if (line == "/help") {
        print_help();
    } else if (line == "/who") {
        if (!send_frame(chat::Type::List, 0)) {
            request_stop();
            return false;
        }
    } else if (line == "/stats") {
        print_stats();
    } else if (line == "/ping") {
        {
            std::lock_guard<std::mutex> lock(g_ping_mutex);
            g_ping_started_ms = chat::now_ms();
        }
        if (!send_frame(chat::Type::Ping, 0)) {
            request_stop();
            return false;
        }
    } else if (line == "/quit" || line == "/exit") {
        return false;
    } else {
        print_line("*** 未知命令 / unknown command: ", line,
                   " (try /help)");
    }
    return true;
}

bool process_line(const std::string &line)
{
    if (line.empty()) {
        return true;
    }
    if (line.front() == '/') {
        return handle_command(line);
    }
    if (line.size() > chat::kMaxMessage) {
        print_line("*** 消息过长 / message too long (max ",
                   chat::kMaxMessage, " bytes)");
        return true;
    }

    const std::uint64_t seq = g_next_seq++;
    if (!send_frame(chat::Type::Msg, seq, line)) {
        request_stop();
        return false;
    }

    std::lock_guard<std::mutex> lock(g_stats_mutex);
    ++g_stats.sent;
    return true;
}

int input_loop()
{
    std::string pending;
    bool discarding_line = false;

    print_line("");
    print_line("=== 已连接 / connected. 输入消息后回车发送 ===");
    print_help();

    while (g_running.load()) {
        fd_set readfds;
        FD_ZERO(&readfds);
        FD_SET(STDIN_FILENO, &readfds);

        timeval timeout {};
        timeout.tv_usec = 200000;

        const int select_result =
            select(STDIN_FILENO + 1, &readfds, nullptr, nullptr, &timeout);
        if (select_result < 0) {
            if (errno == EINTR) {
                continue;
            }
            print_line("*** select(stdin): ", std::strerror(errno));
            request_stop();
            return -1;
        }
        if (select_result == 0 || !FD_ISSET(STDIN_FILENO, &readfds)) {
            continue;
        }

        char buffer[4096];
        const ssize_t n = read(STDIN_FILENO, buffer, sizeof(buffer));
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            print_line("*** 读取输入失败 / read stdin: ",
                       std::strerror(errno));
            request_stop();
            return -1;
        }
        if (n == 0) {
            if (!pending.empty() && !discarding_line) {
                if (!process_line(pending) && !g_running.load()) {
                    return -1;
                }
            }
            print_line("*** 输入结束 / end of input");
            break;
        }

        pending.append(buffer, static_cast<std::size_t>(n));

        if (discarding_line) {
            const std::size_t newline = pending.find('\n');
            if (newline == std::string::npos) {
                pending.clear();
                continue;
            }
            pending.erase(0, newline + 1);
            discarding_line = false;
        }

        for (;;) {
            const std::size_t newline = pending.find('\n');
            if (newline == std::string::npos) {
                if (pending.size() > chat::kMaxMessage) {
                    print_line("*** 行过长，已丢弃 / line too long, discarded");
                    pending.clear();
                    discarding_line = true;
                }
                break;
            }

            std::string line = pending.substr(0, newline);
            pending.erase(0, newline + 1);
            if (!line.empty() && line.back() == '\r') {
                line.pop_back();
            }
            if (!process_line(line)) {
                return g_running.load() ? 0 : -1;
            }
        }
    }

    return g_running.load() ? 0 : -1;
}

bool parse_port(std::string_view text, int &port)
{
    if (text.empty()) {
        return false;
    }

    int value = 0;
    for (const char ch : text) {
        if (ch < '0' || ch > '9') {
            return false;
        }
        value = value * 10 + (ch - '0');
        if (value > 65535) {
            return false;
        }
    }
    if (value < 1) {
        return false;
    }
    port = value;
    return true;
}

void wait_for_handshake()
{
    std::unique_lock<std::mutex> lock(g_state_mutex);
    g_state_cond.wait_for(lock, std::chrono::seconds(10), [] {
        return g_handshake_state.load() != 0;
    });
}

} // 匿名命名空间

int main(int argc, char **argv)
{
    if (argc != 4) {
        std::cerr << "用法/usage: " << argv[0]
                  << " <server-ip> <port> <nickname>\n"
                  << "示例/example: " << argv[0]
                  << " 127.0.0.1 9000 Alice\n";
        return 2;
    }

    int port = 0;
    if (!parse_port(argv[2], port)) {
        std::cerr << "错误/error: invalid port '" << argv[2] << "'\n";
        return 2;
    }
    (void)port;

    std::string reason;
    if (!chat::validate_nick(argv[3], reason)) {
        std::cerr << "错误/error: " << reason << '\n';
        return 2;
    }
    g_nick = argv[3];

    std::signal(SIGPIPE, SIG_IGN);

    print_line("正在连接 ", argv[1], ':', argv[2], " ... / connecting");
    g_socket_fd = connect_to_server(argv[1], argv[2]);
    if (g_socket_fd < 0) {
        return 1;
    }

    std::thread receiver;
    try {
        receiver = std::thread(receiver_thread);
    } catch (const std::exception &exception) {
        std::cerr << "thread error: " << exception.what() << '\n';
        close(g_socket_fd);
        g_socket_fd = -1;
        return 1;
    }

    if (!send_frame(chat::Type::Hello, 0)) {
        request_stop();
        receiver.join();
        close(g_socket_fd);
        return 1;
    }

    wait_for_handshake();
    if (g_handshake_state.load() <= 0) {
        print_line("*** 登录失败或服务器无响应 / login failed or timeout");
        request_stop();
        receiver.join();
        close(g_socket_fd);
        return 1;
    }

    int exit_code = 0;
    if (input_loop() != 0) {
        exit_code = 1;
    }

    if (g_running.load()) {
        send_frame(chat::Type::Quit, 0);
    }
    request_stop();
    receiver.join();
    close(g_socket_fd);
    g_socket_fd = -1;

    print_line("已退出聊天室 / left the chat room");
    return exit_code;
}
