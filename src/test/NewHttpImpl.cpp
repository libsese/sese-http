#include "../Http.h"

#include <sese/Init.h>
#include <sese/Log.h>
#include <sese/security/SSLContextBuilder.h>

int main(int argc, char **argv) {
    sese::initCore(argc, argv);

    auto any = sese::net::IPv4Address::localhost(9956);
    auto ssl = sese::security::SSLContextBuilder::UniqueSSL4Server();
    ssl->importCertFile(PROJECT_PATH "/sese/sese/test/Data/test-ca.crt");
    ssl->importPrivateKeyFile(PROJECT_PATH "/sese/sese/test/Data/test-key.pem");
    MountPointMap mountPoints{{"/", "C:/Users/kaoru/Desktop/html/"}};
    ServletMap servlets;
    FilterCallback tailFilter;
    FilterMap filters;
    ConnectionCallback connectionCallback;
    std::string name = "Hello";
    auto impl = std::make_shared<HttpServiceImpl>(
        any,
        std::move(ssl),
        name,
        30,
        2,
        connectionCallback,
        mountPoints,
        servlets,
        filters,
        tailFilter
        );
    if (!impl->startup()) {
        // SESE_ERROR("exit with {}", impl->getLastError());
        return 0;
    } else {
        while (true) {
            sese::sleep(1);
        }
    }
}
