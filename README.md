# redis-udf
redis_version:4.0.8
mysql:5.6

boost mysql
g++ -shared -o myredis.so -fPIC -I /usr/include/mysql -lboost_serialization -lboost_system -lboost_thread  anet.c redis_client.cpp redis_udf.cpp
