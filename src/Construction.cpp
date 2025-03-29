#include "Http.h"

HttpConnection::HttpConnection(
HttpServiceImpl *service,
asio::io_context &io_context,
const sese::net::IPAddress::Ptr &addr,
size_t timeout)
: service(service), timer(io_context), address(addr) {
    this->timeout = timeout;
    // SESE_INFO("new connection");
}

HttpConnection::~HttpConnection() {
    // SESE_INFO("connection close");
}

HttpConnectionImpl::HttpConnectionImpl(
    HttpServiceImpl *service,
    asio::io_context &io_context,
    const sese::net::IPAddress::Ptr &addr,
    size_t timeout,
    Socket socket) : HttpConnection(service, io_context, addr, timeout), socket(std::move(socket)) {
}

HttpsConnectionImpl::HttpsConnectionImpl(
    HttpServiceImpl *service,
    asio::io_context &io_context,
    const sese::net::IPAddress::Ptr &addr,
    size_t timeout,
    Stream stream) : HttpConnection(service, io_context, addr, timeout), stream(std::move(stream)) {
}

HttpConnectionEx::HttpConnectionEx(
    HttpServiceImpl *service,
    asio::io_context &io_context,
    const sese::net::IPAddress::Ptr &addr,
    size_t timeout
) : service(service), address(addr), timer(io_context), timeout(timeout) {
}

HttpConnectionExImpl::HttpConnectionExImpl(
    HttpServiceImpl *service,
    asio::io_context &io_context,
    const sese::net::IPAddress::Ptr &addr,
    size_t timeout,
    Socket socket
) : HttpConnectionEx(service, io_context, addr, timeout), socket(std::move(socket)) {
}

HttpsConnectionExImpl::HttpsConnectionExImpl(
    HttpServiceImpl *service,
    asio::io_context &io_context,
    const sese::net::IPAddress::Ptr &addr,
    size_t timeout,
    Stream stream) : HttpConnectionEx(service, io_context, addr, timeout), stream(std::move(stream)) {
}

HttpStream::HttpStream(uint32_t id, uint32_t write_window_size, const sese::net::IPAddress::Ptr &addr)
: Handleable(), id(id), endpoint_window_size(write_window_size) {
    using namespace sese::net::http;
    request.setVersion(HttpVersion::VERSION_2);
    response.setVersion(HttpVersion::VERSION_2);
    remote_address = addr;
}
