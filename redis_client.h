#ifndef _REDIS_CLIENT_H
#define _REDIS_CLIENT_H

#include <string>
#include <vector>
#include <set>
#include <stdexcept>
#include <ctime>
#include<stdlib.h>

typedef std::string string_type;
typedef std::vector<string_type> string_vector;
typedef long int_type;
typedef long ssize_t;
	
class redis_error 
{
public:
  redis_error(const string_type & err);
  operator std::string ();
  operator const std::string () const;
private:
  string_type err_;
};

  // Some socket-level I/O or general connection error.

class connection_error : public redis_error
{
public:
  connection_error(const string_type & err);
};

// Redis gave us a reply we were not expecting.
// Possibly an internal error (here or in redis, probably here).

class protocol_error : public redis_error
{
public:
  protocol_error(const string_type & err);
};

// A key that you expected to exist does not in fact exist.

class key_error : public redis_error
{
public:
  key_error(const string_type & err);
};

// A value of an expected type or other semantics was found to be invalid.

class value_error : public redis_error
{
public:
  value_error(const string_type & err);
};

class RedisClient {
	private:
		void send_(const string_type &);
		void recv_ok_reply_();
		string_type recv_single_line_reply_();
		string_type recv_bulk_reply_();
		int_type recv_multi_bulk_reply_(string_vector &);
		int_type recv_bulk_reply_(char);
		string_type read_line(int socket, ssize_t max_size = 2048);
		string_type read_n(int, ssize_t);
	private:
    int socket_;
	public:
		explicit RedisClient(const string_type & host = "localhost", 
                    unsigned int port = 6379);

    ~RedisClient();
    
    void           auth(const string_type & pass);

		void           set(const string_type &,const string_type &);
		string_type    get(const string_type &);
		
		void           hset(const string_type &,const string_type &,const string_type &);
		string_type    hget(const string_type &,const string_type &);
		
		void           hmset(const string_type &,const string_vector &,const string_vector &);
		void           hmget(const string_type &,const string_vector &,string_vector &);
		
		string_type    getset(const string_type &,const string_type &);
		void           del(const string_type &);
		void           save();
		void           bgsave();
		
		
};

static RedisClient *_client = NULL;

RedisClient *init_client_if_isnull();

#endif