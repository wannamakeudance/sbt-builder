FROM hseeberger/scala-sbt:8u171_2.12.6_1.1.6
RUN apt-get update && apt-get install python-pip -y && rm -rf /var/lib/apt/lists/*
RUN pip install awscli
ADD ScalaCode /root/ScalaCode
RUN cd /root/ScalaCode && sbt package && cd /root && rm -rf /root/ScalaCode
ADD http://mirror.bit.edu.cn/apache/spark/spark-2.1.2/spark-2.1.2-bin-hadoop2.7.tgz /root/
RUN cd /root/ && tar xvf spark-2.1.2-bin-hadoop2.7.tgz && rm spark-2.1.2-bin-hadoop2.7.tgz
RUN echo "export PATH=\$PATH:~/spark-2.1.2-bin-hadoop2.7/bin" >>  ~/.bashrc