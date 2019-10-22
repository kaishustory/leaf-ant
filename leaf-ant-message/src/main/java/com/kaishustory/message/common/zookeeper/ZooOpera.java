/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.kaishustory.message.common.zookeeper;

import com.kaishustory.message.common.model.ZooRegisterMethod;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Optional;

import static com.kaishustory.message.common.constants.MessageConstants.METHOD_ADD_COUSUMER;
import static com.kaishustory.message.common.constants.MessageConstants.METHOD_ADD_PRODUCER;

/**
 * Zookeeper操作
 *
 * @author liguoyang
 * @create 2019-07-30 14:14
 **/
@Slf4j
public class ZooOpera {

    private String addr;

    public ZooOpera(String addr) {
        this.addr = addr;
    }

    /**
     * 记录消费端地址
     * @param group 分组
     * @param topic 主题
     * @param host 服务地址
     */
    public void addCousumer(String group, String topic, String host){
        try {
            // 检查节点是否已存在
            if(getCousumerList(group, topic).contains(host)){
                ZooClient.getClient(addr).delete().forPath(String.format("/ks-basic-message/%s/%s/consumer-node/%s", group, topic, host));
                log.info("Zookeeper节点已存在，将原节点删除，重新注册。topic：{}，host：{}", topic, host);
            }
            ZooClient.getClient(addr).create().creatingParentContainersIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(String.format("/ks-basic-message/%s/%s/consumer-node/%s", group, topic, host), "".getBytes());
            ZooReconn.add(new ZooRegisterMethod(addr, METHOD_ADD_COUSUMER, group, topic, host));
            log.info("Zookeeper记录消费端地址成功。Group：{}，Topic：{}, Host：{}", group, topic, host);
        } catch (Exception e) {
            log.error("Zookeeper记录消费端地址失败！Group：{}，Topic：{}, Host：{}", group, topic, host, e);
        }
    }

    /**
     * 记录生产端地址
     * @param group 分组
     * @param topic 主题
     * @param host 服务地址
     */
    public void addProducer(String group, String topic, String host){
        try {
            ZooClient.getClient(addr).create().creatingParentContainersIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(String.format("/ks-basic-message/%s/%s/producer-node/%s", group, topic, host), "".getBytes());
            ZooReconn.add(new ZooRegisterMethod(addr, METHOD_ADD_PRODUCER, group, topic, host));
            log.info("Zookeeper记录生产端地址成功。Group：{}，Topic：{}, Host：{}", group, topic, host);
        } catch (Exception e) {
            log.error("Zookeeper记录生产端地址失败！Group：{}，Topic：{}, Host：{}", group, topic, host, e);
        }
    }

    /**
     * 读取消费者列表
     * @param topic 主题
     * @return 服务列表
     */
    public List<String> getCousumerList(String group, String topic){
        try {
            return ZooClient.getClient(addr).getChildren().forPath(String.format("/ks-basic-message/%s/%s/consumer-node", group, topic));
        }catch (KeeperException.NoNodeException e){
            log.warn("Zookeeper读取消费端地址列表，目前没有服务注册! Group：{}，Topic：{}", group, topic);
            return new ArrayList<>(0);
        }catch (Exception e) {
            log.error("Zookeeper读取消费端地址列表失败！Group：{}，Topic：{}", group, topic, e);
            return new ArrayList<>(0);
        }
    }

    /**
     * 读取生产者列表
     * @param topic 主题
     * @return 服务列表
     */
    public List<String> getProducerList(String group, String topic){
        try {
            return ZooClient.getClient(addr).getChildren().forPath(String.format("/ks-basic-message/%s/%s/producer-node", group, topic));
        }catch (KeeperException.NoNodeException e){
            log.warn("Zookeeper读取生产端地址列表，目前没有服务注册! Group：{}，Topic：{}", group, topic);
            return new ArrayList<>(0);
        } catch (Exception e) {
            log.error("Zookeeper读取生产端地址列表失败！Group：{}，Topic：{}", group, topic, e);
            return new ArrayList<>(0);
        }
    }

    /**
     * 获得局域网地址
     */
    public static Optional<InetAddress> getLocalHostLANAddress() {
        try {
            InetAddress candidateAddress = null;
            // 遍历所有的网络接口
            for (Enumeration ifaces = NetworkInterface.getNetworkInterfaces(); ifaces.hasMoreElements();) {
                NetworkInterface iface = (NetworkInterface) ifaces.nextElement();
                // 在所有的接口下再遍历IP
                for (Enumeration inetAddrs = iface.getInetAddresses(); inetAddrs.hasMoreElements();) {
                    InetAddress inetAddr = (InetAddress) inetAddrs.nextElement();
                    if (!inetAddr.isLoopbackAddress()) {// 排除loopback类型地址
                        if (inetAddr.isSiteLocalAddress()) {
                            // 如果是site-local地址，就是它了
                            return Optional.of(inetAddr);
                        } else if (candidateAddress == null) {
                            // site-local类型的地址未被发现，先记录候选地址
                            candidateAddress = inetAddr;
                        }
                    }
                }
            }
            if (candidateAddress != null) {
                return Optional.of(candidateAddress);
            }
            // 如果没有发现 non-loopback地址.只能用最次选的方案
            InetAddress jdkSuppliedAddress = InetAddress.getLocalHost();
            if (jdkSuppliedAddress == null) {
                throw new UnknownHostException("The JDK InetAddress.getLocalHost() method unexpectedly returned null.");
            }
            return Optional.of(jdkSuppliedAddress);
        } catch (Exception e) {
            log.error("获得网络IP地址发生异常!",e);
            return Optional.empty();
        }
    }
}
