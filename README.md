# the_red

 * 실행하기 전에 install_program.sh 를 실행해서 zookeeper 와 redis를 설치하세요.
 * docker 가 실행가능한 환경이어야 합니다.

## Chapter 1

 * stateless 한 서비스의 예를 보여줍니다. stateless 한 서버는 Load Balancer에 추가하는 방법으로 간단히 확장이 가능합니다.

### geoip
 * ip를 입력하면 해당 국가를 알려주는 예제입니다. 공개된 maxmind 라이브러리를 이용합니다.
  * 독립적으로 동작하는 stateless 한 서비스의 예입니다.

### scrap
 * 입력된 url의 Opengraph를 파싱해서 보여주는 예제입니다.
  * 독립적으로 동작하는 stateless 한 서비스의 예입니다.

## Chapter 2

### loadbalancer
 * loadbalancer 예제는 nginx를 이용해서 간단하게 loadbalancer가 동작하는 것을 보여줍니다.
  * nginx 의 설치가 필요합니다.
```
   sudo apt install -y nginx
```
  * nginx 의 설정은 127.0.0.1:7001 과 127.0.0.1:7002 두 개를 바라보고 있습니다. scrap 서버를 7001, 7002 포트를 사용하도록 두 개를 실행시켜 주면 됩니다.
   * Docker 를 이용하셔도 됩니다. 
   * Docker build
```
   docker build . -t ch1/scrap
```
   * Docker Run
```
   docker run -e ENDPOINTS=0.0.0.0:7001 --network host ch1/scrap
   docker run -e ENDPOINTS=0.0.0.0:7002 --network host ch1/scrap
```


  * 다음 명령으로 실행해볼 수 있습니다. 

### service discovery
 * zookeeper 의 실행이 필요합니다.
 * caller 는 callee 가 추가/제거 되면, zookeeper 를 통해서 callee의 변화를 알림받게 됩니다.
  * callee
   * 실제 Scrap 을 수행하는 서비스입니다.
  * caller
   * callee 에게 Scrap을 요청하는 서비스입니다. 
   * caller 는 call의 목록을 가지고 Round Robin 방식으로 요청을 하게 됩니다.
