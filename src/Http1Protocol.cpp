#include "Http.h"

#include <sese/net/http/HttpUtil.h>
#include <sese/Log.h>

asio::awaitable<void> HttpConnection::handle() {
    do {
        // read header
        bool first = true;
        bool parse_status = false;
        while (!recv_status) {
            char buffer[MTU_VALUE];
            auto readed = co_await asyncRead(buffer, MTU_VALUE);
            // todo bug
            // if (first) {
            //     SESE_INFO("cancel");
            //     timer.cancel();
            // } else {
            //     first = false;
            // }
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
        service->handleFilter(this);
        // read body
        auto expect_length = static_cast<size_t>(sese::toInteger(request.get("content-length", "0")));
        auto real_length = builder.getReadableSize();
        if (real_length && conn_type != ConnType::FILTER) {
            sese::streamMove(&request.getBody(), &builder, real_length);
        }
        builder.freeCapacity();
        while (expect_length > real_length) {
            char buffer[MTU_VALUE];
            auto need = std::min(expect_length - real_length, MTU_VALUE);
            auto readed = co_await asyncRead(buffer, need);
            real_length += readed;
            request.getBody().write(buffer, readed);
        }
        service->handleRequest(this);
        sese::net::http::HttpUtil::sendResponse(&builder, &response);
        co_await writeHeader();
        builder.freeCapacity();
        if (ranges.size() == 1) {
            // one range file
            co_await writeSingleRange();
        } else if (ranges.size() > 1) {
            // multi ranges file
            co_await writeRanges();
        } else {
            co_await writeBody();
        }
        if (keepalive) {
            conn_type = ConnType::NONE;
            is0x0a = false;
            recv_status = false;
            request.clear();
            request.queryArgsClear();
            request.getBody().freeCapacity();
            if (auto cookies = request.getCookies()) {
                cookies->clear();
            }
            response.setCode(200);
            response.clear();
            response.getBody().freeCapacity();
            if (auto cookies = response.getCookies()) {
                cookies->clear();
            }
            // todo bug
            // SESE_INFO("set timeout");
            // timer.expires_after(asio::chrono::seconds(timeout));
            // timer.async_wait([&](const asio::error_code &code) {
            //     SESE_INFO("timeouted");
            //     this->onTimeout(code);
            // });
        }
    } while (keepalive);
}

asio::awaitable<void> HttpConnection::writeHeader() {
    auto &&header = builder;
    auto expect_length = header.getReadableSize();
    size_t real_length = 0;
    while (expect_length != real_length) {
        char buffer[MTU_VALUE];
        auto need = std::min(expect_length - real_length, MTU_VALUE);
        header.peek(buffer, need);
        auto wrote = co_await asyncWrite(buffer, need);
        header.trunc(wrote);
        real_length += wrote;
    }
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
    if (file->setSeek(static_cast<int64_t>(ranges[0].begin), sese::io::Seek::BEGIN)) {
        throw "Failed to call File::setSeek()";
    }
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

asio::awaitable<void> HttpConnection::writeRanges() {
    for (int i = 0; i < ranges.size(); ++i) {
        auto &&range = ranges[i];
        size_t expect_length = range.len;
        size_t real_length = 0;
        if (i == 0) {
            // The first range
            auto subheader = std::string("--") + HTTPD_BOUNDARY + "\r\n" +
                             "content-type: " + content_type + "\r\n" +
                             "content-range: " + range.toString(filesize) + "\r\n\r\n";
            co_await asyncWrite(subheader.data(), subheader.length());
        } else {
            // The ranges in between
            auto subheader = std::string("\r\n--") + HTTPD_BOUNDARY + "\r\n" +
                             "content-type: " + content_type + "\r\n" +
                             "content-range: " + range.toString(filesize) + "\r\n\r\n";
            co_await asyncWrite(subheader.data(), subheader.length());
        }
        if (file->setSeek(static_cast<int64_t>(range.begin), sese::io::Seek::BEGIN)) {
            throw "Failed to call File::setSeek()";
        }
        while (expect_length != real_length) {
            char buffer[MTU_VALUE];
            auto need = std::min(expect_length - real_length, MTU_VALUE);
            file->read(buffer, need);
            auto wrote = co_await asyncWrite(buffer, need);
            assert(wrote == need);
            real_length += wrote;
        }
        if (i == ranges.size() - 1) {
            // The last range
            auto subheader = std::string("\r\n--") + HTTPD_BOUNDARY + "--\r\n";
            co_await asyncWrite(subheader.data(), subheader.length());
        }
    }
}
