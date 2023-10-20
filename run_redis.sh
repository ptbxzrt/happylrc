kill -9 $(pidof redis-server)

# proxy 1
./3rd_party/redis/bin/redis-server --daemonize yes --bind 0.0.0.0 --port 10000
./3rd_party/redis/bin/redis-server --daemonize yes --bind 0.0.0.0 --port 10001
./3rd_party/redis/bin/redis-server --daemonize yes --bind 0.0.0.0 --port 10002
./3rd_party/redis/bin/redis-server --daemonize yes --bind 0.0.0.0 --port 10003
./3rd_party/redis/bin/redis-server --daemonize yes --bind 0.0.0.0 --port 10004
./3rd_party/redis/bin/redis-server --daemonize yes --bind 0.0.0.0 --port 10005
./3rd_party/redis/bin/redis-server --daemonize yes --bind 0.0.0.0 --port 10006
./3rd_party/redis/bin/redis-server --daemonize yes --bind 0.0.0.0 --port 10007
./3rd_party/redis/bin/redis-server --daemonize yes --bind 0.0.0.0 --port 10008
./3rd_party/redis/bin/redis-server --daemonize yes --bind 0.0.0.0 --port 10009
 
# proxy 2
./3rd_party/redis/bin/redis-server --daemonize yes --bind 0.0.0.0 --port 10100
./3rd_party/redis/bin/redis-server --daemonize yes --bind 0.0.0.0 --port 10101
./3rd_party/redis/bin/redis-server --daemonize yes --bind 0.0.0.0 --port 10102
./3rd_party/redis/bin/redis-server --daemonize yes --bind 0.0.0.0 --port 10103
./3rd_party/redis/bin/redis-server --daemonize yes --bind 0.0.0.0 --port 10104
./3rd_party/redis/bin/redis-server --daemonize yes --bind 0.0.0.0 --port 10105
./3rd_party/redis/bin/redis-server --daemonize yes --bind 0.0.0.0 --port 10106
./3rd_party/redis/bin/redis-server --daemonize yes --bind 0.0.0.0 --port 10107
./3rd_party/redis/bin/redis-server --daemonize yes --bind 0.0.0.0 --port 10108
./3rd_party/redis/bin/redis-server --daemonize yes --bind 0.0.0.0 --port 10109

# proxy 3
./3rd_party/redis/bin/redis-server --daemonize yes --bind 0.0.0.0 --port 10200
./3rd_party/redis/bin/redis-server --daemonize yes --bind 0.0.0.0 --port 10201
./3rd_party/redis/bin/redis-server --daemonize yes --bind 0.0.0.0 --port 10202
./3rd_party/redis/bin/redis-server --daemonize yes --bind 0.0.0.0 --port 10203
./3rd_party/redis/bin/redis-server --daemonize yes --bind 0.0.0.0 --port 10204
./3rd_party/redis/bin/redis-server --daemonize yes --bind 0.0.0.0 --port 10205
./3rd_party/redis/bin/redis-server --daemonize yes --bind 0.0.0.0 --port 10206
./3rd_party/redis/bin/redis-server --daemonize yes --bind 0.0.0.0 --port 10207
./3rd_party/redis/bin/redis-server --daemonize yes --bind 0.0.0.0 --port 10208
./3rd_party/redis/bin/redis-server --daemonize yes --bind 0.0.0.0 --port 10209

# proxy 4
./3rd_party/redis/bin/redis-server --daemonize yes --bind 0.0.0.0 --port 10300
./3rd_party/redis/bin/redis-server --daemonize yes --bind 0.0.0.0 --port 10301
./3rd_party/redis/bin/redis-server --daemonize yes --bind 0.0.0.0 --port 10302
./3rd_party/redis/bin/redis-server --daemonize yes --bind 0.0.0.0 --port 10303
./3rd_party/redis/bin/redis-server --daemonize yes --bind 0.0.0.0 --port 10304
./3rd_party/redis/bin/redis-server --daemonize yes --bind 0.0.0.0 --port 10305
./3rd_party/redis/bin/redis-server --daemonize yes --bind 0.0.0.0 --port 10306
./3rd_party/redis/bin/redis-server --daemonize yes --bind 0.0.0.0 --port 10307
./3rd_party/redis/bin/redis-server --daemonize yes --bind 0.0.0.0 --port 10308
./3rd_party/redis/bin/redis-server --daemonize yes --bind 0.0.0.0 --port 10309