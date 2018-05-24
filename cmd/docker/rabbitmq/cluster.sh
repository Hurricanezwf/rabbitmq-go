#!/bin/bash

# 基础配置
DefaultUser="zwf"
DefaultPassword="zwf"
ImageName="rabbitmq:3.7.4-management"


docker pull ${ImageName}

# 创建集群网络(不然不同结点不能互通)
docker network create rabbitmq-net



# 创建并启动容器
docker run -d \
--name=rabbitmq1 \
-p 5673:5672 \
-p 15673:15672 \
-e RABBITMQ_NODENAME=rabbitmq1 \
-e RABBITMQ_ERLANG_COOKIE='zhouwenfeng' \
-e RABBITMQ_DEFAULT_USER=${DefaultUser} \
-e RABBITMQ_DEFAULT_PASS=${DefaultPassword} \
-h rabbitmq1 \
--net=rabbitmq-net \
${ImageName}


docker run -d \
--name=rabbitmq2 \
-p 5674:5672 \
-p 15674:15672 \
-e RABBITMQ_NODENAME=rabbitmq2 \
-e RABBITMQ_ERLANG_COOKIE='zhouwenfeng' \
-e RABBITMQ_DEFAULT_USER=${DefaultUser} \
-e RABBITMQ_DEFAULT_PASS=${DefaultPassword} \
-h rabbitmq2 \
--net=rabbitmq-net \
${ImageName}


docker run -d \
--name=rabbitmq3 \
-p 5675:5672 \
-p 15675:15672 \
-e RABBITMQ_NODENAME=rabbitmq3 \
-e RABBITMQ_ERLANG_COOKIE='zhouwenfeng' \
-e RABBITMQ_DEFAULT_USER=${DefaultUser} \
-e RABBITMQ_DEFAULT_PASS=${DefaultPassword} \
-h rabbitmq3 \
--net=rabbitmq-net \
${ImageName}




# 将结点2和3加入集群
docker exec rabbitmq2 bash -c \
    "rabbitmqctl stop_app && \
     rabbitmqctl reset && \
     rabbitmqctl join_cluster rabbitmq1@rabbitmq1 && \
     rabbitmqctl start_app"

docker exec rabbitmq3 bash -c \
    "rabbitmqctl stop_app && \
     rabbitmqctl reset && \
     rabbitmqctl join_cluster rabbitmq1@rabbitmq1 && \
     rabbitmqctl start_app"


# 查看集群情况
docker exec -it rabbitmq1 bash -c "rabbitmqctl cluster_status"



# 通过正则表达式设置指定队列为镜像队列
docker exec -it rabbitmq1 rabbitmqctl \
    set_policy ha-all 'queue.+' '{"ha-mode":"all"}'
