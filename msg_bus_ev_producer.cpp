/**
 *  msg_bus_ev_producer.cpp
 *
 *  A simple test program using the LibEv handler over a TLS connection
 *
 *  Compile this with: g++ -l:libev.so -lssl -lamqpcpp  -std=c++11 -g -Wall -Wextra -march=x86-64 msg_bus_ev_producer.cpp -o msg_bus_ev_producer
 */

#include <amqpcpp.h>
#include <ev.h>
#include <amqpcpp/libev.h>
#include <memory>
#include <unistd.h>
#include <chrono>

typedef std::chrono::high_resolution_clock HiResClock;
typedef std::chrono::milliseconds          Milliseconds;

class my_channel
{
public:
    my_channel(std::shared_ptr<AMQP::TcpConnection> conn,
               std::string exchName, std::string payld)
    : _channel(new AMQP::TcpChannel(conn.get()))
    , _exchange(exchName)
    , _payload(payld)
    {
        _channel->onReady(std::bind(&my_channel::onConnected, this));
        _channel->onError(std::bind(&my_channel::onError, this));
    }

    ~my_channel()
    {}

private:
    void onConnected()
    {
        _channel->declareExchange(_exchange, AMQP::fanout, AMQP::noack)
                .onSuccess(std::bind(&my_channel::onDeclareExchange, this))
                .onError(std::bind(&my_channel::onDeclareExchangeFailed, this, std::placeholders::_1));
    }

    void onError()
    {
        std::cout << "Channel Failure." << std::endl;
    }

    void onDeclareExchange()
    {
        std::cout << "Exchange Declared." << std::endl;
        startTest();
    }

    void startTest()
    {
        std::cout << "Beginning Test..." << std::endl;
        HiResClock::time_point startTime = HiResClock::now();
        while ((std::chrono::duration_cast<Milliseconds>(HiResClock::now()-startTime)).count() < 10000.0)
        {
            _channel->publish(_exchange, "", _payload.c_str());
            usleep(5);
        }
    }

    void onDeclareExchangeFailed(const char* message)
    {
        std::cerr << message << std::endl;
        onDeclareExchange();
    }

    std::shared_ptr<AMQP::TcpChannel> _channel;
    std::string _exchange;
    std::string _payload;
};

int main()
{
    // Create the payload being sent.
    int32_t packetSize = 1024;

    // Payload for first channel
    std::string payload;
    payload.resize(packetSize);
    for (int i=0; i < packetSize; ++i)
    {
        payload[i] = ('0' + (i%10));
    }

    std::string exchange_name = "ampexchange";
    const char* host = "localhost";
    int port = 5671; // Using 5671 for TLS, change to 5672 for non-TLS
    std::string login = "guest";
    std::string password = "guest";
    std::string vhost = "/";
    
    // Initialize SSL Library before the connection is created.
    SSL_library_init();

    auto *poll = EV_DEFAULT;
    std::unique_ptr<AMQP::LibEvHandler> my_handler;
    my_handler.reset(new AMQP::LibEvHandler(poll));
 
    std::shared_ptr<AMQP::TcpConnection> my_connection;
    my_connection.reset(new AMQP::TcpConnection
                           (my_handler.get(),
                            AMQP::Address(host,
                                          port,
                                          AMQP::Login(login, password),
                                          vhost,
                                          (port == 5671) ? true : false)
                            )
                       );
    
    my_channel channel1(my_connection, exchange_name, payload);

    std::cout << "Starting ev_run" << std::endl;
    ev_run(poll, 0);    
 
    return (0);
}
