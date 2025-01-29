#include "Http.h"

HttpConnection::HttpConnection(
    HttpServiceImpl *service,
    asio::io_context &io_context,
    const sese::net::IPAddress::Ptr &addr)
    : service(service) {
}

HttpConnectionImpl::HttpConnectionImpl(
    HttpServiceImpl *service,
    asio::io_context &io_context,
    const sese::net::IPAddress::Ptr &addr,
    Socket socket) : HttpConnection(service, io_context, addr), socket(std::move(socket)) {
}

asio::awaitable<size_t> HttpConnectionImpl::asyncRead(void *buffer, size_t size) {
    co_return co_await this->socket.async_read_some(asio::buffer(buffer, size), asio::use_awaitable);
}

asio::awaitable<size_t> HttpConnectionImpl::asyncWrite(const void *buffer, size_t size) {
    // co_return co_await this->socket.async_write_some(asio::buffer(buffer, size), asio::use_awaitable);
    auto block_size = size;
    auto p = buffer;
    while (block_size) {
        auto wrote = co_await this->socket.async_write_some(asio::buffer(p, block_size), asio::use_awaitable);
        p += wrote;
        block_size -= wrote;
    }
    co_return size;
}

HttpsConnectionImpl::HttpsConnectionImpl(
    HttpServiceImpl *service,
    asio::io_context &io_context,
    const sese::net::IPAddress::Ptr &addr,
    Stream stream) : HttpConnection(service, io_context, addr), stream(std::move(stream)) {
}

asio::awaitable<size_t> HttpsConnectionImpl::asyncRead(void *buffer, size_t size) {
    co_return co_await this->stream.async_read_some(asio::buffer(buffer, size), asio::use_awaitable);
}

asio::awaitable<size_t> HttpsConnectionImpl::asyncWrite(const void *buffer, size_t size) {
    // co_return co_await this->stream.async_write_some(asio::buffer(buffer, size), asio::use_awaitable);
    auto block_size = size;
    auto p = buffer;
    while (block_size) {
        auto wrote = co_await this->stream.async_write_some(asio::buffer(buffer, size), asio::use_awaitable);
        p += wrote;
        block_size -= wrote;
    }
    co_return size;
}
