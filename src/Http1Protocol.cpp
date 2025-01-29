#include "Http.h"

#include <sese/net/http/HttpUtil.h>

asio::awaitable<void> HttpConnection::handle() {
    do {
        char buffer[MTU_VALUE];
        // read header
        bool parse_status = false;
        while (!recv_status) {
            auto readed = co_await asyncRead(buffer, MTU_VALUE);
            builder.write(buffer, readed);
            for (int i = 0; i < readed; ++i) {
                if (is0x0a && buffer[i] == '\r') {
                    is0x0a = false;
                    recv_status = true;
                    parse_status = sese::net::http::HttpUtil::recvRequest(&builder, &request);
                    break;
                }
                is0x0a = buffer[i] == '\n';
            }
        }
        if (!parse_status) {
            break;
        }
        // read body
        auto expect_length = static_cast<size_t>(sese::toInteger(request.get("content-length", "0")));
        auto real_length = builder.getReadableSize();
        if (real_length && conn_type != ConnType::FILTER) {
            sese::streamMove(&request.getBody(), &builder, real_length);
        }
        builder.freeCapacity();
        while (expect_length > real_length) {
            auto need = std::min(expect_length - real_length, MTU_VALUE);
            auto readed = co_await asyncRead(buffer, need);
            real_length += readed;
            request.getBody().write(buffer, readed);
        }
        // todo handle
        if (ranges.size() == 1) {
            // one range file
            co_await writeSingleRange();
        } else if (ranges.size() > 1) {
            // multi ranges file
            // todo move to handle
            if (file->setSeek(static_cast<int64_t>(range_iterator->begin), sese::io::Seek::BEGIN)) {
                throw "Failed to call File::setSeek()";
            }
            co_await writeRanges();
        } else {
            co_await writeBody();
        }
    } while (keepalive);
}

asio::awaitable<void> HttpConnection::writeBody() {
    auto &&body = response.getBody();
    auto expect_length = body.getReadableSize();
    size_t real_length = 0;
    while (expect_length != real_length) {
        char buffer[MTU_VALUE];
        auto need = std::min(expect_length - real_length, MTU_VALUE);
        body.peek(buffer, need);
        auto wrote = co_await asyncWrite(buffer, need);
        body.trunc(wrote);
        real_length += wrote;
    }
}

asio::awaitable<void> HttpConnection::writeSingleRange() {
    auto expect_length = ranges[0].len;
    size_t real_length = 0;
    while (expect_length != real_length) {
        char buffer[MTU_VALUE];
        auto need = std::min(expect_length - real_length, MTU_VALUE);
        this->file->read(buffer, need);
        auto wrote = co_await asyncWrite(buffer, need);
        assert(wrote == need);
        real_length += need;
    }
}
