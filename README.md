# Leaf Ant
Leaf Ant基于Canal实现，将MySQL数据同步至其他数据源（ElasticSearch、Redis、MQ、MySQL）
# 功能
### ElasticSearch同步
1. 支持MySQL单表、多表模式近实时同步至ElasticSearch
2. 支持MySQL数据全量导入ElasticSearch
### Redis同步
1. 支持MySQL近实时同步至Redis
2. 支持MySQL数据全量导入Redis
### MQ同步
1. 支持MySQL数据变更事件，转发至阿里MQ（未来支持RocketMQ）
### MySQL
1. 支持MySQL近实时同步至另一个MySQL实例
2. 支持MySQL数据全量导入另一个MySQL实例
# 部署
1. 下载工程、解压：https://github.com/kaishustory/leaf-ant/releases/download/0.9.0/leafant-0.9.0.zip
2. 包含三个项目：leafant-subscribe、leafant-transform、leafant-web，建议生产环境分不同服务器部署
3. 补充 conf/application.properties 配置，包括zookeeper地址、mongodb地址、阿里MQ地址（未来替换为RocketMQ）、redis地址。
4. 启动项目（最简单，不建议生产环境使用）：java -jar leafant-subscribe-0.9.0-SNAPSHOT.jar
5. 依次启动三个项目 leafant-subscribe、leafant-transform、leafant-web
6. 访问数据源配置页面 http://127.0.0.1:8080
