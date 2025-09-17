#include "streamit/common/http_health_server.h"
#include <cstring>
#include <netinet/in.h>
#include <sstream>
#include <sys/socket.h>
#include <unistd.h>

namespace streamit::common {

HttpHealthServer::HttpHealthServer(const std::string& host, uint16_t port, std::shared_ptr<HealthCheckManager> manager)
    : host_(host), port_(port), manager_(std::move(manager)), running_(false) {
}

HttpHealthServer::~HttpHealthServer() {
  Stop();
}

bool HttpHealthServer::Start() noexcept {
  if (running_.load()) {
    return true;
  }

  running_.store(true);
  server_thread_ = std::make_unique<std::thread>(&HttpHealthServer::ServerLoop, this);
  return true;
}

bool HttpHealthServer::Stop() noexcept {
  if (!running_.load()) {
    return true;
  }

  running_.store(false);
  if (server_thread_ && server_thread_->joinable()) {
    server_thread_->join();
  }
  return true;
}

bool HttpHealthServer::IsRunning() const noexcept {
  return running_.load();
}

void HttpHealthServer::ServerLoop() {
  int server_socket = socket(AF_INET, SOCK_STREAM, 0);
  if (server_socket < 0) {
    return;
  }

  int opt = 1;
  setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

  struct sockaddr_in address;
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = INADDR_ANY;
  address.sin_port = htons(port_);

  if (bind(server_socket, (struct sockaddr*)&address, sizeof(address)) < 0) {
    close(server_socket);
    return;
  }

  if (listen(server_socket, 5) < 0) {
    close(server_socket);
    return;
  }

  while (running_.load()) {
    struct sockaddr_in client_addr;
    socklen_t client_len = sizeof(client_addr);

    int client_socket = accept(server_socket, (struct sockaddr*)&client_addr, &client_len);
    if (client_socket < 0) {
      continue;
    }

    HandleRequest(client_socket);
    close(client_socket);
  }

  close(server_socket);
}

void HttpHealthServer::HandleRequest(int client_socket) {
  char buffer[1024];
  ssize_t bytes_read = read(client_socket, buffer, sizeof(buffer) - 1);
  if (bytes_read <= 0) {
    return;
  }

  buffer[bytes_read] = '\0';
  std::string request(buffer);

  // Simple HTTP request parsing
  if (request.find("GET /live") != std::string::npos) {
    // Liveness check - always return OK if server is running
    SendResponse(client_socket, 200, "OK");
  } else if (request.find("GET /ready") != std::string::npos) {
    // Readiness check - run health checks
    if (manager_) {
      auto result = manager_->RunChecks();
      if (result.status == HealthStatus::HEALTHY) {
        SendResponse(client_socket, 200, "OK");
      } else {
        SendResponse(client_socket, 503, "Service Unavailable: " + result.message);
      }
    } else {
      SendResponse(client_socket, 200, "OK");
    }
  } else if (request.find("GET /metrics") != std::string::npos) {
    // Metrics endpoint - return Prometheus metrics
    SendResponse(client_socket, 200, "# Metrics endpoint - implement Prometheus export here");
  } else {
    SendResponse(client_socket, 404, "Not Found");
  }
}

void HttpHealthServer::SendResponse(int client_socket, int status_code, const std::string& body) {
  std::ostringstream response;
  response << "HTTP/1.1 " << status_code << " ";

  switch (status_code) {
  case 200:
    response << "OK";
    break;
  case 404:
    response << "Not Found";
    break;
  case 503:
    response << "Service Unavailable";
    break;
  default:
    response << "Internal Server Error";
    break;
  }

  response << "\r\n";
  response << "Content-Type: text/plain\r\n";
  response << "Content-Length: " << body.length() << "\r\n";
  response << "Connection: close\r\n";
  response << "\r\n";
  response << body;

  std::string response_str = response.str();
  send(client_socket, response_str.c_str(), response_str.length(), 0);
}

} // namespace streamit::common
