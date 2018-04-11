/**
 *  msg_asio_producer.cpp
 *
 *  A simple test program using the Boost ASIO handler over a TLS connection
 *
 *  Compile this with: g++ -I /opt/boost/Boost-1.62.0/include -L/opt/boost/Boost-1.62.0/lib/ -l:libboost_system.so -lssl -lamqpcpp  -std=c++11 -g -Wall -Wextra -march=x86-64  msg_bus_asio_producer.cpp -o msg_bus_asio_producer
 */

#include <boost/asio/io_service.hpp>
#include <boost/asio/deadline_timer.hpp>
#include <amqpcpp.h>
#include <amqpcpp/libboostasio.h>
#include <memory>

static int cnt = 0;

void publishMsg(boost::asio::deadline_timer* sendT,
                std::shared_ptr<AMQP::TcpChannel> ch,
                std::string exch)
{
    std::cout << "sending data (" << cnt++ << ")" << std::endl;
    std::string msgPayload = "ABCDEFGHIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVWXYZ";
    AMQP::Envelope env(msgPayload.data(), msgPayload.size());
    ch->publish(exch, "", env);
    sendT->expires_from_now(boost::posix_time::milliseconds(5));
    sendT->async_wait(std::bind(publishMsg, sendT, ch, exch));
}

int main()
{  
    std::string exchange_name = "ampexchange";
    const char* host = "localhost";
    int port = 5671;
    std::string login = "guest";
    std::string password = "guest";
    std::string vhost = "/";
    
    SSL_library_init();

    boost::asio::io_service ioService; 
    
    AMQP::LibBoostAsioHandler my_handler(ioService);
    
    AMQP::TcpConnection my_connection(&my_handler,
                                      AMQP::Address(host,
                                                    port,
                                                    AMQP::Login(login, password),
                                                    vhost,
                                                    (port == 5671) ? true : false));

    std::shared_ptr<AMQP::TcpChannel> my_channel;
    my_channel.reset(new AMQP::TcpChannel(&my_connection));

    // Declare callbacks
    auto onDeclareExchange = [&my_channel](){
        std::cout << "Exchange Declared." << std::endl;
    };

    auto onDeclareExchangeFailed = [&my_channel, &onDeclareExchange](const char* message){
        std::cerr << message << std::endl;
    };

    // Kick off the process...
    my_channel->declareExchange(exchange_name, AMQP::fanout, AMQP::noack)
            .onSuccess(onDeclareExchange)
            .onError(onDeclareExchangeFailed);

    // Prime the sending of data through the ioService
    boost::asio::deadline_timer sendTimer(ioService, boost::posix_time::milliseconds(5));
    sendTimer.async_wait([&](const boost::system::error_code& /*ec*/)
    {
       publishMsg(&sendTimer, my_channel, exchange_name);
    });

    ioService.run();  

    return (0);
}
