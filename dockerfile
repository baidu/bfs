FROM centos:latest
MAINTAINER ystyle "lxy5266@live.com"
RUN yum -y groupinstall 'Development Tools' && yum -y install zlib-devel wget fuse-devel git psmisc &&\
    cd / && git clone https://github.com/baidu/bfs.git &&\
    # sed -i '4a FUSE_PATH=/usr/include/fuse/' /bfs/Makefile &&\
    cd /bfs && ./build.sh && cd sandbox && ./deploy.sh &&\
    yum -y groupremove 'Development Tools' && yum clean all &&\
    cd ../ && rm -rf thirdparty thirdsrc .build .git src
EXPOSE 8827 8828 8829
WORKDIR /bfs/sandbox/
CMD ./start_bfs.sh && tail -f /dev/null
