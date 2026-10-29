#include "protocol.h"

#include <algorithm>
#include <arpa/inet.h>
#include <cerrno>
#include <csignal>
#include <cstdio>
#include <cstring>
#include <exception>
#include <iostream>
#include <mutex>
#include <netinet/in.h>
#include <new>
#include <sstream>
#include <string>
#include <string_view>
#include <sys/socket.h>
#include <sys/time.h>
#include <thread>
#include <unistd.h>
#include <vector>

namespace {

constexpr int kMaxClients = 64;
constexpr int kSendTimeoutSeconds = 5;

struct Client {
    int fd = -1;
    std::string nick;
    std::string peer_ip;
    unsigned int peer_port = 0;
    std::uint64_t last_client_seq = 0;
};

std::mutex g_clients_mutex;
std::mutex g_log_mutex;
std::vector<Client *> g_clients;

template <typename... Args>
void log_msg(const Args &...args)
{
    std::ostringstream message;
    (message << ... << args);
    const std::string timestamp = chat::format_time(chat::now_ms());

    std::lock_guard<std::mutex> lock(g_log_mutex);
    std::cout << '[' << timestamp << "] " << message.str() << '\n'
              << std::flush;
}

Client *find_client_locked(std::string_view nick)
{
    const auto it = std::find_if(
        g_clients.begin(), g_clients.end(),
        [nick](const Client *client) { return client->nick == nick; });
    return it == g_clients.end() ? nullptr : *it;
}

/*
 * 所有发往已注册客户端的数据都在持有 g_clients_mutex 时进行。
 * 这样可以串行化帧发送，并防止某个客户端在其他线程使用其 socket 时被删除。
 * 注册完成之前，Client 只由它自己的线程持有，因此此时无锁发送是安全的。
 */
bool send_to_locked(Client &client, chat::Type type, std::uint64_t seq,
                    std::string_view sender, std::uint64_t timestamp_ms,
                    std::string_view body)
{
    if (chat::send_frame(client.fd, type, seq, sender, timestamp_ms, body) == 0) {
        return true;
    }

    log_msg("send to ", client.nick, " failed: ", std::strerror(errno));
    shutdown(client.fd, SHUT_RDWR);
    return false;
}

void send_error_to_client(Client &client, std::string_view message)
{
    send_to_locked(client, chat::Type::Error, 0, "server", chat::now_ms(),
                   message);
}

void broadcast_locked(chat::Type type, std::uint64_t seq,
                      std::string_view sender, std::uint64_t timestamp_ms,
                      std::string_view body, const Client *exclude)
{
    for (Client *client : g_clients) {
        if (client != exclude) {
            send_to_locked(*client, type, seq, sender, timestamp_ms, body);
        }
    }
}

void unregister_client(Client &client)
{
    bool removed = false;
    int online = 0;

    {
        std::lock_guard<std::mutex> lock(g_clients_mutex);
        const auto it =
            std::find(g_clients.begin(), g_clients.end(), &client);
        if (it != g_clients.end()) {
            g_clients.erase(it);
            removed = true;
        }
        online = static_cast<int>(g_clients.size());

        if (removed && online > 0) {
            const std::string body =
                client.nick + " 离开聊天室 / " + client.nick + " left (" +
                std::to_string(online) + " online)";
            broadcast_locked(chat::Type::Leave, 0, client.nick,
                             chat::now_ms(), body, nullptr);
        }
    }

    if (removed) {
        log_msg(client.nick, " disconnected (", online, " online)");
    }
}

bool register_client(Client &client, std::string_view nick)
{
    std::string reason;

    if (!chat::validate_nick(nick, reason)) {
        send_error_to_client(client, reason);
        return false;
    }

    int online = 0;
    int rejection = 0; /* 1 = 昵称重复，2 = 房间已满 */
    {
        std::lock_guard<std::mutex> lock(g_clients_mutex);
        if (find_client_locked(nick) != nullptr) {
            rejection = 1;
        } else if (g_clients.size() >=
                   static_cast<std::size_t>(kMaxClients)) {
            rejection = 2;
        } else {
            client.nick.assign(nick);
            g_clients.push_back(&client);
            online = static_cast<int>(g_clients.size());

            const std::string welcome =
                "欢迎 " + client.nick + "！当前在线 " +
                std::to_string(online) + " 人。 / Welcome " + client.nick +
                "! " + std::to_string(online) +
                " user(s) online. Type /help for commands.";
            send_to_locked(client, chat::Type::Welcome, 0, "server",
                           chat::now_ms(), welcome);

            const std::string join_body =
                client.nick + " 加入聊天室 / " + client.nick + " joined (" +
                std::to_string(online) + " online)";
            broadcast_locked(chat::Type::Join, 0, client.nick,
                             chat::now_ms(), join_body, &client);
        }
    }

    if (rejection == 1) {
        send_error_to_client(client,
                             "昵称已被使用 / nickname already in use");
        return false;
    }
    if (rejection == 2) {
        send_error_to_client(client, "聊天室已满 / chat room is full");
        return false;
    }

    log_msg(client.peer_ip, ':', client.peer_port, " joined as ",
            client.nick, " (", online, " online)");
    return true;
}

void handle_list(Client &client, const chat::Frame &frame)
{
    std::lock_guard<std::mutex> lock(g_clients_mutex);

    std::string body;
    for (const Client *member : g_clients) {
        if (!body.empty()) {
            body += ", ";
        }
        body += member->nick;
    }
    send_to_locked(client, chat::Type::ListReply, frame.seq, "server",
                   chat::now_ms(), body);
}

void handle_message(Client &client, const chat::Frame &frame)
{
    if (frame.body.empty()) {
        std::lock_guard<std::mutex> lock(g_clients_mutex);
        send_error_to_client(client, "空消息 / empty message");
        return;
    }
    if (frame.body.size() > chat::kMaxMessage) {
        std::lock_guard<std::mutex> lock(g_clients_mutex);
        send_error_to_client(client, "消息过长 / message too long");
        return;
    }
    if (frame.seq == 0) {
        std::lock_guard<std::mutex> lock(g_clients_mutex);
        send_error_to_client(client,
                             "MSG sequence must be positive");
        return;
    }
    if (frame.body.find('\0') != std::string::npos) {
        std::lock_guard<std::mutex> lock(g_clients_mutex);
        send_error_to_client(client,
                             "消息包含 NUL 字节 / message contains NUL");
        return;
    }

    if (client.last_client_seq != 0 &&
        frame.seq > client.last_client_seq + 1) {
        log_msg("WARN ", client.nick, " sequence gap: ",
                client.last_client_seq, " -> ", frame.seq);
    } else if (frame.seq <= client.last_client_seq) {
        log_msg("WARN ", client.nick, " duplicate/old sequence: ",
                client.last_client_seq, " -> ", frame.seq);
    }
    if (frame.seq > client.last_client_seq) {
        client.last_client_seq = frame.seq;
    }

    const std::uint64_t now = chat::now_ms();
    std::lock_guard<std::mutex> lock(g_clients_mutex);
    broadcast_locked(chat::Type::Msg, frame.seq, client.nick, now,
                     frame.body, nullptr);
}

/* 返回 false 表示应关闭该连接。 */
bool handle_client_frame(Client &client, const chat::Frame &frame)
{
    switch (frame.type) {
    case chat::Type::Msg:
        handle_message(client, frame);
        return true;

    case chat::Type::List:
        handle_list(client, frame);
        return true;

    case chat::Type::Ping:
        {
            std::lock_guard<std::mutex> lock(g_clients_mutex);
            send_to_locked(client, chat::Type::Pong, frame.seq, "server",
                           chat::now_ms(), {});
        }
        return true;

    case chat::Type::Quit:
        return false;

    default:
        {
            const std::string message =
                "客户端不能发送 " + std::string(chat::type_name(frame.type)) +
                " / client may not send " +
                std::string(chat::type_name(frame.type));
            std::lock_guard<std::mutex> lock(g_clients_mutex);
            send_error_to_client(client, message);
        }
        return true;
    }
}

void client_thread(Client *client)
{
    const int fd = client->fd;
    bool registered = false;
    std::string error;

    log_msg("connection from ", client->peer_ip, ':', client->peer_port);

    for (;;) {
        chat::Frame frame;
        const chat::RecvStatus status = chat::recv_frame(fd, frame, error);

        if (status == chat::RecvStatus::Closed) {
            break;
        }
        if (status == chat::RecvStatus::ProtocolError) {
            log_msg(registered ? client->nick : std::string("unregistered"),
                    " protocol error: ", error);
            if (registered) {
                std::lock_guard<std::mutex> lock(g_clients_mutex);
                send_error_to_client(*client, error);
            } else {
                send_error_to_client(*client, error);
            }
            break;
        }
        if (status != chat::RecvStatus::Ok) {
            log_msg(registered ? client->nick : std::string("unregistered"),
                    " receive error: ", error);
            break;
        }

        if (!registered) {
            if (frame.type != chat::Type::Hello || !frame.body.empty()) {
                send_error_to_client(*client,
                                     "请先发送 HELLO 进行登录 / "
                                     "send HELLO first");
                break;
            }
            if (!register_client(*client, frame.sender)) {
                break;
            }
            registered = true;
        } else if (!handle_client_frame(*client, frame)) {
            break;
        }
    }

    if (registered) {
        unregister_client(*client);
    }
    close(fd);
    delete client;
}

int create_listener(int port)
{
    const int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        std::perror("socket");
        return -1;
    }

    int yes = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) < 0) {
        std::perror("setsockopt(SO_REUSEADDR)");
        close(fd);
        return -1;
    }

    sockaddr_in address {};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_ANY);
    address.sin_port = htons(static_cast<std::uint16_t>(port));

    if (bind(fd, reinterpret_cast<sockaddr *>(&address), sizeof(address)) < 0) {
        std::perror("bind");
        close(fd);
        return -1;
    }
    if (listen(fd, 32) < 0) {
        std::perror("listen");
        close(fd);
        return -1;
    }
    return fd;
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

} // 匿名命名空间

int main(int argc, char **argv)
{
    int port = chat::kDefaultPort;

    if (argc > 2) {
        std::cerr << "用法/usage: " << argv[0] << " [port]\n";
        return 2;
    }
    if (argc == 2 && !parse_port(argv[1], port)) {
        std::cerr << "错误/error: invalid port '" << argv[1] << "'\n";
        return 2;
    }

    std::signal(SIGPIPE, SIG_IGN);

    const int listen_fd = create_listener(port);
    if (listen_fd < 0) {
        return 1;
    }

    std::cout << "chat_server listening on 0.0.0.0:" << port << '\n'
              << "press Ctrl+C to stop / 按 Ctrl+C 停止\n"
              << std::flush;

    for (;;) {
        sockaddr_in peer {};
        socklen_t peer_len = sizeof(peer);
        const int fd =
            accept(listen_fd, reinterpret_cast<sockaddr *>(&peer), &peer_len);

        if (fd < 0) {
            if (errno == EINTR) {
                continue;
            }
            std::perror("accept");
            continue;
        }

        timeval timeout {};
        timeout.tv_sec = kSendTimeoutSeconds;
        if (setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout,
                       sizeof(timeout)) < 0) {
            std::perror("setsockopt(SO_SNDTIMEO)");
        }

        auto *client = new (std::nothrow) Client;
        if (client == nullptr) {
            std::perror("new");
            close(fd);
            continue;
        }
        client->fd = fd;

        char ip[INET_ADDRSTRLEN] {};
        if (inet_ntop(AF_INET, &peer.sin_addr, ip, sizeof(ip)) == nullptr) {
            client->peer_ip = "?";
        } else {
            client->peer_ip = ip;
        }
        client->peer_port = ntohs(peer.sin_port);

        try {
            std::thread(client_thread, client).detach();
        } catch (const std::exception &exception) {
            std::cerr << "pthread_create/thread error: " << exception.what()
                      << '\n';
            close(fd);
            delete client;
        }
    }
}
