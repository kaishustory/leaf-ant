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
1. 支持MySQL数据变更事件，转发至RocketMQ
### MySQL
1. 支持MySQL近实时同步至另一个MySQL实例
2. 支持MySQL数据全量导入另一个MySQL实例
# 部署
1. 下载工程、解压：https://github.com/kaishustory/leaf-ant/releases/download/0.9.0/leafant-0.9.0.zip
2. 包含三个项目：leafant-subscribe、leafant-transform、leafant-web，建议生产环境分不同服务器部署
3. 补充 conf/application.properties 配置，包括zookeeper地址、mongodb地址、RocketMQ、redis地址。
4. 启动项目（最简单，不建议生产环境使用）：java -jar leafant-subscribe-0.9.0-SNAPSHOT.jar
5. 依次启动三个项目 leafant-subscribe、leafant-transform、leafant-web
6. 访问数据源配置页面 http://127.0.0.1:8080
# 架构图
![avatar](https://raw.githubusercontent.com/kaishustory/leaf-ant/master/.material/数据同步方案-数据同步流程.png)
# ElasticSearch同步配置
1. 登录系统（访客账号：guest，密码：guest）
![avatar](https://raw.githubusercontent.com/kaishustory/leaf-ant/master/.material/1.%20登录页面.png)

2. ElasticSearch同步配置列表
![avatar](https://raw.githubusercontent.com/kaishustory/leaf-ant/master/.material/2.%20ElasticSearch配置列表.png)

3. 创建同步配置【第一步：选择MySQL表】
![avatar](https://raw.githubusercontent.com/kaishustory/leaf-ant/master/.material/3.%20选择MySQL数据表.png)

4. 创建同步配置【第二步：配置ElasticSearch结构和地址】
![avatar](https://raw.githubusercontent.com/kaishustory/leaf-ant/master/.material/4.%20配置ElasticSearch结构.png)

5. 同步历史数据
6. 开启数据实时同步
![avatar](https://raw.githubusercontent.com/kaishustory/leaf-ant/master/.material/5.%20依次初始化和开启同步.png)



## Contributors ✨

Thanks goes to these wonderful people ([emoji key](https://allcontributors.org/docs/en/emoji-key)):

<!-- ALL-CONTRIBUTORS-LIST:START - Do not remove or modify this section -->
<!-- prettier-ignore -->
<table>
  <tr>
    <td align="center"><a href="https://github.com/liguoyangik"><img src="https://avatars3.githubusercontent.com/u/55503412?v=4" width="100px;" alt="李国洋"/><br /><sub><b>李国洋</b></sub></a><br /><a href="#design-liguoyangik" title="Design">🎨</a> <a href="https://github.com/kaishustory/leaf-ant/commits?author=liguoyangik" title="Code">💻</a></td>
  </tr>
</table>

<!-- ALL-CONTRIBUTORS-LIST:END -->
