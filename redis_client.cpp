#include "redis_client.h"
#include "anet.h"

#include <sstream>

#include <algorithm>
#include <iostream>
#include <ctime>
#include <unistd.h>
#include <cstring>
#include <cassert>

#include <sys/errno.h>
#include <sys/socket.h>
using namespace std;

const string_type status_reply_ok("OK");
const string_type prefix_status_reply_error("-ERR ");
const char prefix_status_reply_value = '+';
const char prefix_single_bulk_reply = '$';
const char prefix_multi_bulk_reply = '*';
const char prefix_int_reply = ':';
const string_type missing_value("**nonexistent-key**");
const string whitespace(" \f\n\r\t\v");
const string CRLF("\r\n");

template <typename T>
T value_from_string(const string & data)
{
  T value;

  istringstream iss(data);
  iss >> value;
  if (iss.fail()) 
    throw value_error("invalid number");

  return value;
}

string_type & rtrim(string & str, const string & ws = whitespace)
{
  string_type::size_type pos = str.find_last_not_of(ws);
  str.erase(pos + 1);
  return str;
}

class makecmd
{
public:
  explicit makecmd(const string_type & initial, bool finalize = false) 
  {
    buffer_ << initial;
    if (!finalize)
      buffer_ << " ";
  }

  template <typename T> 
  makecmd & operator<<(T const & datum)
  {
    buffer_ <<" "<< datum;
    return *this;
  }

  template <typename T>
  makecmd & operator<<(const std::vector<T> & data) 
  {
  	buffer_ << " ";
    size_t n = data.size();
    for (size_t i = 0; i < n; ++i)
    {
      buffer_ << data[i];
      if (i < n - 1)
          buffer_ << " ";
    }
    return *this;
  }

  operator std::string ()
  {
    buffer_ << CRLF;
    return buffer_.str();
  }

private:
  std::ostringstream buffer_;
};

redis_error::redis_error(const string_type & err) : err_(err) 
{
}

redis_error::operator std::string ()
{
  return err_;
}

redis_error::operator const std::string () const
{
  return err_;
}

connection_error::connection_error(const string_type & err) : redis_error(err)
{
}

protocol_error::protocol_error(const string_type & err) : redis_error(err)
{
}

key_error::key_error(const string_type & err) : redis_error(err)
{
}

value_error::value_error(const string_type & err) : redis_error(err)
{
}

RedisClient::RedisClient(const string_type & host, unsigned int port)
{
	char err[ANET_ERR_LEN];
    socket_ = anetTcpConnect(err, const_cast<char*>(host.c_str()), port);
    if (socket_ == ANET_ERR) 
      throw connection_error(err);
    anetTcpNoDelay(NULL, socket_);
#ifdef DEBUG
		std::cout<<"open redis success"<<std::endl;
#endif
}    

RedisClient::~RedisClient()
{
	if (socket_ != ANET_ERR)
      close(socket_);
#ifdef DEBUG
		std::cout<<"close redis success"<<std::endl;
#endif
}  

void  RedisClient::auth(const string_type & pass)
{
	send_(makecmd("AUTH") << pass);
  recv_ok_reply_();
}

void RedisClient::set(const string_type & key,const string_type & value)
{
	send_(makecmd("SET") << key << value);
	recv_ok_reply_();
}
string_type RedisClient::get(const string_type & key){
	send_(makecmd("GET") << key);
	return recv_bulk_reply_();
}

void RedisClient::hset(const string_type & key,const string_type & field,const string_type & value){
	send_(makecmd("HSET") << key << field << value);
	//return :0
	recv_bulk_reply_(prefix_int_reply);
}

string_type RedisClient::hget(const string_type & key,const string_type & field){
	send_(makecmd("HGET") << key << field);
	return recv_bulk_reply_();
}

void RedisClient::del(const string_type & key){
	send_(makecmd("DEL") << key);
	recv_bulk_reply_(prefix_int_reply);
}

void RedisClient::save(){
	send_(makecmd("SAVE"));
	recv_ok_reply_();
}

void RedisClient::bgsave(){
	send_(makecmd("BGSAVE"));
	recv_single_line_reply_();
}

void RedisClient::hmset(const string_type & key,const string_vector & fields,const string_vector & values){
	makecmd maker("HMSET");
	maker << key;
	if(fields.size() != values.size() || fields.size() <= 0){
		throw protocol_error("invalid arguments");
	}
	for(int i = 0;i < fields.size();i++){
		maker << fields[i] << values[i];
	}
	send_(maker);
	recv_ok_reply_();
}

void RedisClient::hmget(const string_type & key,const string_vector & fields,string_vector & out){
	send_(makecmd("HMGET")<<key<<fields);
	recv_multi_bulk_reply_(out);
}
	
string_type RedisClient::getset(const string_type & key,const string_type & value){
	send_(makecmd("GETSET") << key << value);
	return recv_bulk_reply_();
}

void RedisClient::recv_ok_reply_() 
{
  if (recv_single_line_reply_() != status_reply_ok) 
    throw protocol_error("expected OK response");
}  

string_type RedisClient::recv_single_line_reply_()
{
  string_type line = read_line(socket_);

  if (line.empty())
    throw protocol_error("empty single line reply");
  if (line.find(prefix_status_reply_error) == 0) 
  {
    string_type error_msg = line.substr(prefix_status_reply_error.length());
    if (error_msg.empty()) 
      error_msg = "unknown error";
    throw protocol_error(error_msg);
  }

  if (line[0] != prefix_status_reply_value)
    throw protocol_error("unexpected prefix for status reply");
  return line.substr(1);
}

string_type RedisClient::recv_bulk_reply_() 
{
  int_type length = recv_bulk_reply_(prefix_single_bulk_reply);

  if (length == -1)
    return missing_value;

  int_type real_length = length + 2;    // CRLF

  string_type data = read_n(socket_, real_length);

  if (data.empty())
    throw protocol_error("invalid bulk reply data; empty");

  if (data.length() != static_cast<string_type::size_type>(real_length))
    throw protocol_error("invalid bulk reply data; data of unexpected length");

  data.erase(data.size() - 2);

  return data;
}

int_type RedisClient::recv_multi_bulk_reply_(string_vector & out)
{
  int_type length = recv_bulk_reply_(prefix_multi_bulk_reply);
#ifdef DEBUG
  	std::cout<<"len is "<<length<<std::endl;
  	#endif
  if (length == -1)
    throw key_error("no such key");

  for (int_type i = 0; i < length; ++i){
  	string_type ret = recv_bulk_reply_();
  	#ifdef DEBUG
  	std::cout<<"ret is "<<ret<<std::endl;
  	#endif
  	out.push_back(ret);
  }

  return length;
}

void RedisClient::send_(const string_type & msg)
{
#ifdef DEBUG
  std::cout<< "send cmd "<<msg<<std::endl;
#endif

  if (anetWrite(socket_, const_cast<char *>(msg.data()), msg.size()) == -1)
    throw connection_error(strerror(errno));
}

int_type RedisClient::recv_bulk_reply_(char prefix)
{
  string line = read_line(socket_);

#ifdef DEBUG
  std::cout<<"line is "<<line<<std::endl;
#endif

  if (line[0] != prefix)
    throw protocol_error("unexpected prefix for bulk reply");

  return value_from_string<int_type>(line.substr(1));
}

string_type RedisClient::read_line(int socket, ssize_t max_size) 
{
  assert(socket > 0);
  assert(max_size > 0);

  std::ostringstream oss;

  enum { buffer_size = 64 };
  char buffer[buffer_size];
  memset(buffer, 0, buffer_size);

  ssize_t total_bytes_read = 0;
  bool found_delimiter = false;

  while (total_bytes_read < max_size && !found_delimiter)
  {
    // Peek at what's available.

    ssize_t bytes_received = 0;
    do bytes_received = recv(socket, buffer, buffer_size, MSG_PEEK);
    while (bytes_received < 0 && errno == EINTR);

    if (bytes_received == 0)
      throw connection_error("connection was closed");

    // Some data is available; Length might be < buffer_size.
    // Look for newline in whatever was read though.

    char * eol = static_cast<char *>(memchr(buffer, '\n', bytes_received));

    // If found, write data from the buffer to the output string.
    // Else, write the entire buffer and continue reading more data.

    ssize_t to_read = bytes_received;

    if (eol) 
    {
      to_read = eol - buffer + 1;
      oss.write(buffer, to_read);
      found_delimiter = true;
    }
    else
      oss.write(buffer, bytes_received);

    // Now read from the socket to remove the peeked data from the socket's
    // read buffer.  This will not block since we've peeked already and know
    // there's data waiting.  It might fail if we were interrupted however.

    do bytes_received = recv(socket, buffer, to_read, 0);
    while (bytes_received < 0 && errno == EINTR);
  }

  // Construct final line string. Remove trailing CRLF-based whitespace.

  string_type line = oss.str();
  return rtrim(line, CRLF);
}

string_type RedisClient::read_n(int socket, ssize_t n)
{
  char * buffer = new char[n + 1];
  buffer[n] = '\0';

  char * bp = buffer;
  ssize_t bytes_read = 0;

  while (bytes_read != n) 
  {
    ssize_t bytes_received = 0;
    do bytes_received = recv(socket, bp, n - (bp - buffer), 0);
    while (bytes_received < 0 && errno == EINTR);

    if (bytes_received == 0)
      throw connection_error("connection was closed");

    bytes_read += bytes_received;
    bp         += bytes_received;
  }

  string_type str(buffer);
  delete [] buffer;
  return str;
}

RedisClient *init_client_if_isnull()
{
    if(!_client){
        const char* c_host = getenv("REDIS_HOST"); // 获取操作系统变量
        const char * c_pass = getenv("REDID_PASS");
        //string_type host = "changhua0208.cn";
        if(!c_host)
            c_host = "changhua0208.cn";
        _client = new RedisClient(c_host,6379);
        if(!c_pass)
        		c_pass = "changhua.jiang";
        _client->auth(c_pass);
    }
    return _client;
}