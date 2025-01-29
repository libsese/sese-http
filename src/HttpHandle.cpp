#include "Http.h"
#include "sese/Util.h"
#include "sese/Log.h"
#include "sese/net/http/HttpUtil.h"

#include <filesystem>

void HttpServiceImpl::handleFilter(Handleable *handleable) {
    auto &&req = handleable->request;
    auto &&resp = handleable->response;
    for (auto &&[uri_prefix, callback]: filters) {
        if (sese::text::StringBuilder::startsWith(req.getUri(), uri_prefix)) {
            if (callback(req, resp)) {
                handleable->conn_type = Handleable::ConnType::NONE;
            } else {
                handleable->conn_type = Handleable::ConnType::FILTER;
                break;
            }
        }
    }
}

void HttpServiceImpl::handleRequest(Handleable *handleable) {
    handleable->stopwatch.stop();
    auto &&req = handleable->request;
    auto &&resp = handleable->response;
    std::filesystem::path filename;

    // Filter matching
    // for (auto &&[uri_prefix, callback]: filters) {
    //     if (text::StringBuilder::startsWith(req.getUri(), uri_prefix)) {
    //         handleable->conn_type = ConnType::FILTER;
    //         if (callback(req, resp)) {
    //             handleable->conn_type = ConnType::NONE;
    //         }
    //     }
    // }

    // Mount point matching
    if (handleable->conn_type == Handleable::ConnType::NONE) {
        for (auto &&[uri_prefix, mount_point]: mount_points) {
            if (sese::text::StringBuilder::startsWith(req.getUri(), uri_prefix)) {
                handleable->conn_type = Handleable::ConnType::FILE_DOWNLOAD;
                filename = mount_point + "/" + req.getUri().substr(uri_prefix.length());
                // Confirm the file name and proceed to the next step
                break;
            }
        }
    }

    if (handleable->conn_type == Handleable::ConnType::NONE) {
        auto iterator = servlets.find(req.getUri());
        if (iterator == servlets.end()) {
            resp.setCode(404);
        } else {
            auto ctx = sese::net::http::HttpServletContext(req, resp, handleable->remote_address);
            iterator->second.invoke(ctx);
            handleable->conn_type = Handleable::ConnType::CONTROLLER;
        }
        resp.set("content-length", std::to_string(resp.getBody().getLength()));
    } else if (handleable->conn_type == Handleable::ConnType::FILE_DOWNLOAD) {
        if (!exists(filename) ||
            !is_regular_file(filename) ||
            is_directory(filename)) {
            resp.setCode(404);
            resp.set("content-length", std::to_string(resp.getBody().getLength()));
            handleable->conn_type = Handleable::ConnType::NONE;
            goto uni_handle;
        }

        handleable->file = sese::io::File::create(filename.string(), sese::io::File::B_READ);
        if (!handleable->file) {
            resp.setCode(500);
            resp.set("content-length", std::to_string(resp.getBody().getLength()));
            goto uni_handle;
        }

        if (filename.has_extension()) {
            auto ext = filename.extension().string().substr(1);
            auto type = sese::net::http::HttpUtil::content_type_map.find(ext);
            if (type == sese::net::http::HttpUtil::content_type_map.end()) {
                resp.set("content-type", handleable->content_type);
            } else {
                resp.set("content-type", type->second);
                handleable->content_type = type->second;
            }
        }

        handleable->filesize = file_size(filename);
        handleable->ranges = sese::net::http::Range::parse(req.get("Range", ""), handleable->filesize);
        if (handleable->ranges.empty()) {
            // No range file, manually set range
            handleable->ranges.emplace_back(0, handleable->filesize);
            // Normal documents
            resp.set("content-length", std::to_string(handleable->filesize));
            resp.setCode(200);
        } else if (handleable->ranges.size() == 1) {
            // Single range file
            // Check ranges
            if (handleable->ranges[0].begin + handleable->ranges[0].len > handleable->filesize) {
                resp.setCode(416);
                resp.set("content-length", std::to_string(resp.getBody().getLength()));
                goto uni_handle;
            }
            // content-length
            resp.set("content-length", std::to_string(handleable->ranges[0].len));
            resp.setCode(206);
        } else {
            // Multi-range file
            size_t content_length = 0;
            // Validate ranges and calculate total length
            for (auto &&item: handleable->ranges) {
                if (item.begin + item.len > handleable->filesize) {
                    resp.setCode(416);
                    resp.set("content-length", std::to_string(resp.getBody().getLength()));
                    goto uni_handle;
                }
                content_length += 12 +
                                  strlen(HTTPD_BOUNDARY) +
                                  strlen("Content-Type: ") +
                                  handleable->content_type.length() +
                                  strlen("Content-Range: ") +
                                  item.toStringLength(handleable->filesize) +
                                  item.len;
            }
            content_length += 6 + strlen(HTTPD_BOUNDARY);
            // content-type
            resp.set("content-type", std::string("multipart/byteranges; boundary=") + HTTPD_BOUNDARY);
            // content-length
            resp.set("content-length", std::to_string(content_length));
            resp.setCode(206);
        }

        auto last_modified = last_write_time(filename);
        uint64_t time = sese::to_time_t(last_modified) * 1000 * 1000;
        resp.set("last-modified", sese::text::DateTimeFormatter::format(sese::DateTime(time, 0), TIME_GREENWICH_MEAN_PATTERN));
    }

uni_handle:
    if (req.getVersion() == sese::net::http::HttpVersion::VERSION_1_1) {
        auto keepalive_str = req.get("connection", "close");
        handleable->keepalive = sese::strcmpDoNotCase(keepalive_str.c_str(), "keep-alive");

        if (handleable->keepalive) {
            resp.set("connection", "keep-alive");
            resp.set("keep-alive", "timeout=" + std::to_string(timeout));
        }
    }
    resp.set("server", this->serv_name);
    resp.set("accept-range", "bytes");
    if (tail_filter && (resp.getCode() != 200 && resp.getCode() != 201)) {
        if(tail_filter(req, resp)) {
            resp.set("content-length", std::to_string(resp.getBody().getReadableSize()));
            handleable->conn_type = Handleable::ConnType::CONTROLLER;
        }
    }
    SESE_INFO("{} {} {} in {}ms", sese::net::http::requestTypeToString(req.getType()), req.getUri(), resp.getCode(), handleable->stopwatch.stop().getTotalMilliseconds());
}

